defmodule Logflare.Source.SlackHookServer.Client do
  @moduledoc false
  require Logger

  alias Logflare.Sources
  alias LogflareWeb.Router.Helpers, as: Routes
  alias LogflareWeb.Endpoint

  @middleware [Tesla.Middleware.JSON]

  def new() do
    middleware =
      [
        {Tesla.Middleware.Retry,
         delay: 500,
         max_retries: 10,
         max_delay: 4_000,
         should_retry: fn
           {:ok, %{status: status}} when status in 400..599 -> true
           {:ok, _} -> false
           {:error, _} -> true
         end}
      ] ++ @middleware

    adapter = {Tesla.Adapter.Mint, timeout: 60_000, mode: :passive}

    Tesla.client(middleware, adapter)
  end

  def post(client, source, rate, recent_events \\ []) do
    body = slack_post_body(source, rate, recent_events)
    url = source.slack_hook_url
    request = Tesla.post(client, url, body)

    case request do
      {:ok, %Tesla.Env{status: 200} = response} ->
        {:ok, response}

      {:ok, %Tesla.Env{body: "invalid_blocks"} = response} ->
        resp = prep_tesla_resp_for_log(response)

        Logger.warn("Slack hook response: invalid_blocks",
          slackhook_response: resp,
          slackhook_request: %{url: url, body: inspect(body)}
        )

        {:error, response}

      {:ok, %Tesla.Env{body: "no_service"} = response} ->
        resp = prep_tesla_resp_for_log(response)

        Logger.warn("Slack hook response: no_service", slackhook_response: resp)

        case Sources.delete_slack_hook_url(source) do
          {:ok, _source} ->
            Logger.warn("Slack hook url deleted.")

          {:error, _changeset} ->
            Logger.error("Error deleting Slack hook url.")
        end

        {:error, response}

      {:ok, %Tesla.Env{} = response} ->
        resp = prep_tesla_resp_for_log(response)

        Logger.warn("Slack hook error!", slackhook_response: resp)

        {:error, response}

      {:error, response} ->
        resp = prep_tesla_resp_for_log(response)

        Logger.warn("Slack hook error!", slackhook_response: resp)
        {:error, response}
    end
  end

  defp prep_tesla_resp_for_log(req_or_resp) do
    Map.from_struct(req_or_resp)
    |> Map.drop([:__client__, :__module__, :headers, :opts, :query])
    |> Map.put(:body, inspect(req_or_resp.body))
  end

  defp prep_recent_events(recent_events, rate) do
    cond do
      0 == rate ->
        slack_no_events_message()

      rate in 1..3 ->
        Enum.take(recent_events, -rate)
        |> Enum.map(fn x ->
          slack_event_message(x)
        end)
        |> Enum.join("\r")

      true ->
        Enum.take(recent_events, -3)
        |> Enum.map(fn x ->
          slack_event_message(x)
        end)
        |> Enum.join("\r")
    end
  end

  defp slack_post_body(source, rate, recent_events) do
    prepped_recent_events = prep_recent_events(recent_events, rate)

    source_link =
      LogflareWeb.Endpoint.static_url() <> Routes.source_path(Endpoint, :show, source.id)

    main_message = "#{rate} new event(s) for your source `#{source.name}`"

    %{
      text: main_message,
      blocks: [
        %{
          type: "section",
          text: %{
            type: "mrkdwn",
            text: main_message
          }
        },
        %{
          type: "section",
          text: %{
            type: "mrkdwn",
            text: "*Recent Events*\r#{prepped_recent_events}"
          },
          accessory: %{
            type: "button",
            text: %{
              type: "plain_text",
              text: "See all events"
            },
            url: source_link,
            style: "primary"
          }
        },
        %{
          type: "context",
          elements: [
            %{
              type: "mrkdwn"
            }
          ]
        }
      ]
    }
  end

  defp slack_event_message(event) do
    time = Kernel.floor(event.body["timestamp"] / 1_000_000)

    "<!date^#{time}^{date_pretty} at {time_secs}|#{event.ingested_at}>\r>#{event.body["event_message"]}"
  end

  defp slack_no_events_message() do
    time = DateTime.to_unix(DateTime.utc_now())
    "<!date^#{time}^{date_pretty} at {time_secs}|blah>\r>Your events will show up here!"
  end
end
