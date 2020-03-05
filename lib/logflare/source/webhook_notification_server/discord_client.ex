defmodule Logflare.Source.WebhookNotificationServer.DiscordClient do
  require Logger

  alias LogflareWeb.Router.Helpers, as: Routes
  alias LogflareWeb.Endpoint

  @middleware [Tesla.Middleware.JSON]

  @adapter Tesla.Adapter.Hackney

  def new() do
    middleware =
      [
        #  {Tesla.Middleware.Retry,
        #   delay: 500,
        #   max_retries: 10,
        #   max_delay: 4_000,
        #   should_retry: fn
        #     {:ok, %{status: status}} when status in 400..599 -> true
        #     {:ok, _} -> false
        #     {:error, _} -> true
        #   end}
      ] ++ @middleware

    adapter = {@adapter, pool: __MODULE__, recv_timeout: 60_000}

    Tesla.client(middleware, adapter)
  end

  def post(client, source, rate, recent_events \\ []) do
    prepped_recent_events = prep_recent_events(recent_events, rate)

    source_link = Endpoint.static_url() <> Routes.source_path(Endpoint, :show, source.id)

    payload = %{
      content: "#{rate} new event(s) 🚨",
      username: "Logflare",
      avatar_url: "https://logflare.app/images/logos/logflare-logo.png",
      embeds: [
        %{
          author: %{
            name: source.name,
            url: source_link,
            icon_url: "https://logflare.app/images/logos/logflare-logo.png"
          },
          title: "Recent Event(s)",
          description: "Visit [#{source.name}](#{source_link}) to stream, search and chart.",
          footer: %{text: "Thanks for using Logflare 🙏"},
          fields: prepped_recent_events
        }
      ]
    }

    send(client, source.webhook_notification_url, payload)
  end

  defp send(client, url, payload) do
    case Tesla.post(client, url, payload) do
      {:ok, %Tesla.Env{status: 200} = response} ->
        {:ok, response}

      {:ok, %Tesla.Env{status: 204} = response} ->
        {:ok, response}

      {:ok, %Tesla.Env{} = response} ->
        resp = prep_tesla_resp_for_log(response)

        Logger.warn("Webhook error!", webhook_response: resp)
        {:error, response}

      {:error, response} ->
        Logger.warn("Webhook error!", webhook_response: %{error: response})
        {:error, response}
    end
  end

  defp prep_tesla_resp_for_log(response) do
    Map.from_struct(response)
    |> Map.drop([:__client__, :__module__, :headers, :opts, :query])
  end

  defp prep_recent_events(recent_events, rate) do
    cond do
      0 == rate ->
        [%{name: "[timestamp]", value: "```[log event message]```"}]

      rate in 1..3 ->
        Enum.take(recent_events, -rate)
        |> Enum.map(fn x ->
          discord_event_message(x)
        end)

      true ->
        Enum.take(recent_events, -3)
        |> Enum.map(fn x ->
          discord_event_message(x)
        end)
    end
  end

  defp discord_event_message(x) do
    timestamp = DateTime.from_unix!(x.body.timestamp, :microsecond) |> DateTime.to_string()

    %{name: timestamp, value: "```#{x.body.message}```"}
  end
end
