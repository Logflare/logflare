defmodule Logflare.Backends.Spool.Queue.PubSub do
  @moduledoc false

  @behaviour Logflare.Backends.Spool.Queue

  alias GoogleApi.PubSub.V1.Api.Projects
  alias GoogleApi.PubSub.V1.Connection
  alias GoogleApi.PubSub.V1.Model.AcknowledgeRequest
  alias GoogleApi.PubSub.V1.Model.ModifyAckDeadlineRequest
  alias GoogleApi.PubSub.V1.Model.PublishRequest
  alias GoogleApi.PubSub.V1.Model.PubsubMessage
  alias GoogleApi.PubSub.V1.Model.PullRequest

  # For PubSub, the queue_name IS the full resource path:
  # - Consumer (subscription): "projects/PROJECT/subscriptions/NAME"
  # - Producer (topic):        "projects/PROJECT/topics/NAME"
  # resolve/1 validates the format and returns it unchanged.
  # Base URL override for emulators: config :google_api_pub_sub, base_url: "http://localhost:8085"
  @impl Logflare.Backends.Spool.Queue
  def resolve(resource_path) do
    case resource_path do
      "projects/" <> _ -> {:ok, resource_path}
      _ -> {:error, "PubSub resource path must start with 'projects/': #{resource_path}"}
    end
  end

  @impl Logflare.Backends.Spool.Queue
  def receive(subscription, _opts) do
    with {:ok, conn} <- build_conn(),
         {:ok, %{receivedMessages: messages}} <-
           Projects.pubsub_projects_subscriptions_pull(conn, subscription,
             body: %PullRequest{maxMessages: 1}
           ) do
      normalized =
        (messages || [])
        |> Enum.map(fn %{ackId: ack_id, message: %{data: data}} ->
          %{id: ack_id, body: Base.decode64!(data)}
        end)

      {:ok, normalized}
    end
  end

  @impl Logflare.Backends.Spool.Queue
  def ack(subscription, ack_id) do
    with {:ok, conn} <- build_conn() do
      case Projects.pubsub_projects_subscriptions_acknowledge(conn, subscription,
             body: %AcknowledgeRequest{ackIds: [ack_id]}
           ) do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl Logflare.Backends.Spool.Queue
  def nack(subscription, ack_id) do
    with {:ok, conn} <- build_conn() do
      case Projects.pubsub_projects_subscriptions_modify_ack_deadline(conn, subscription,
             body: %ModifyAckDeadlineRequest{ackIds: [ack_id], ackDeadlineSeconds: 0}
           ) do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl Logflare.Backends.Spool.Queue
  def publish(topic, body) do
    with {:ok, conn} <- build_conn() do
      case Projects.pubsub_projects_topics_publish(conn, topic,
             body: %PublishRequest{messages: [%PubsubMessage{data: Base.encode64(body)}]}
           ) do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp build_conn do
    token =
      case Application.get_env(:goth, :json) do
        nil ->
          # No credentials — assume local emulator which doesn't validate tokens
          "local-dev-token"

        _ ->
          case Goth.fetch(Logflare.Spool.Goth) do
            {:ok, %{token: t}} -> t
            {:error, reason} -> throw({:goth_fetch_error, reason})
          end
      end

    {:ok, Connection.new(token)}
  catch
    {:goth_fetch_error, reason} -> {:error, reason}
  end
end
