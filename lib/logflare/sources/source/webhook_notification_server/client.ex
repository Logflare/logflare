defmodule Logflare.Sources.Source.WebhookNotificationServer.Client do
  @moduledoc false
  require Logger

  @middleware [Tesla.Middleware.JSON]

  def new do
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

    adapter = {Tesla.Adapter.Mint, timeout: 60_000, mode: :passive}

    Tesla.client(middleware, adapter)
  end

  def post(client, source, rate, recent_events \\ []) do
    prepped_recent_events = prep_recent_events(recent_events)

    payload = %{
      rate: rate,
      source_name: source.name,
      source: source.token,
      recent_events: prepped_recent_events
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
        Logger.warning("Webhook error!",
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
        Logger.warning("Webhook error!", webhook_response: %{error: inspect(response)})
        {:error, response}
    end
  end

  defp prep_recent_events(recent_events) do
    Enum.take(recent_events, -5)
    |> Enum.map(fn x -> x.body["event_message"] end)
  end
end
