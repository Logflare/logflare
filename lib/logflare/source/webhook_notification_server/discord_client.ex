defmodule Logflare.Source.WebhookNotificationServer.DiscordClient do
  @moduledoc false
  require Logger

  alias LogflareWeb.Router.Helpers, as: Routes
  alias LogflareWeb.Endpoint

  @middleware [Tesla.Middleware.JSON]

  def new() do
    middleware =
      [
        #  {Tesla.Middleware.Retry,
        #   delay: 500,
        #   max_retries: 5,
        #   max_delay: 4_000,
        #   should_retry: fn
        #     {:ok, %{status: status}} when status in 500..599 -> true
        #     {:ok, _} -> false
        #     {:error, _} -> true
        #   end},
        {Tesla.Middleware.Query, [wait: "true"]}
      ] ++ @middleware

    adapter = {Tesla.Adapter.Mint, timeout: 60_000, mode: :passive}

    Tesla.client(middleware, adapter)
  end

  def post(client, source, rate, recent_events \\ []) do
    prepped_recent_events = prep_recent_events(recent_events, rate)

    source_link = Endpoint.static_url() <> Routes.source_path(Endpoint, :show, source.id)

    payload = %{
      username: "Logflare",
      avatar_url: "https://logflare.app/images/logos/logflare-logo.png",
      embeds: [
        %{
          author: %{
            name: source.name,
            url: source_link,
            icon_url: "https://logflare.app/images/logos/logflare-logo.png"
          },
          title: "#{rate} new event(s)",
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
        Logger.warn("Webhook error!",
          webhook_request: %{url: url, body: inspect(payload)},
          webhook_response: %{
            body: inspect(response.body),
            method: response.method,
            status: response.status,
            url: response.url
          }
        )

        {:error, response}

      {:error, response} ->
        Logger.warn("Webhook error!", webhook_response: %{error: inspect(response)})
        {:error, response}
    end
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
    timestamp = DateTime.from_unix!(x.body["timestamp"], :microsecond) |> DateTime.to_string()
    {message, _} = String.split_at(x.body["event_message"], 1018)

    %{name: timestamp, value: "```#{message}```"}
  end
end
