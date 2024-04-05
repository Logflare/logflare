defmodule Logflare.Source.SlackHookServer.Client do
  @moduledoc false
  require Logger

  alias Logflare.Sources
  alias LogflareWeb.Router.Helpers, as: Routes
  alias LogflareWeb.Endpoint
  alias Logflare.Backends.Adaptor.SlackAdaptor

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

        Logger.warning("Slack hook response: invalid_blocks",
          slackhook_response: resp,
          slackhook_request: %{url: url, body: inspect(body)}
        )

        {:error, response}

      {:ok, %Tesla.Env{body: "no_service"} = response} ->
        resp = prep_tesla_resp_for_log(response)

        Logger.warning("Slack hook response: no_service", slackhook_response: resp)

        case Sources.delete_slack_hook_url(source) do
          {:ok, _source} ->
            Logger.warning("Slack hook url deleted.")

          {:error, _changeset} ->
            Logger.error("Error deleting Slack hook url.")
        end

        {:error, response}

      {:ok, %Tesla.Env{} = response} ->
        resp = prep_tesla_resp_for_log(response)

        Logger.warning("Slack hook error!", slackhook_response: resp)

        {:error, response}

      {:error, response} ->
        resp = prep_tesla_resp_for_log(response)

        Logger.warning("Slack hook error!", slackhook_response: resp)
        {:error, response}
    end
  end

  defp prep_tesla_resp_for_log(req_or_resp) do
    Map.from_struct(req_or_resp)
    |> Map.drop([:__client__, :__module__, :headers, :opts, :query])
    |> Map.put(:body, inspect(req_or_resp.body))
  end

  defp take_events(recent_events, rate) do
    cond do
      0 == rate ->
        []

      rate in 1..3 ->
        recent_events
        |> Enum.take(-rate)

      true ->
        recent_events
        |> Enum.take(-3)
    end
  end

  def slack_post_body(source, rate, recent_events) do
    event_bodies =
      take_events(recent_events, rate)
      |> Enum.map(fn le ->
        {:ok, dt} = DateTime.from_unix(le.body["timestamp"], :microsecond)
        %{DateTime.to_string(dt) => le.body["event_message"]}
      end)

    source_link =
      LogflareWeb.Endpoint.static_url() <> Routes.source_path(Endpoint, :show, source.id)

    main_message = "*Recent Events* - #{rate} new event(s) for your source `#{source.name}`"

    SlackAdaptor.to_body(event_bodies,
      context: main_message,
      button_link: %{
        text: "See all events",
        url: source_link
      }
    )
  end
end
