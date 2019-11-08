defmodule Logflare.Source.WebhookNotificationServer.Client do
  require Logger

  @middleware [Tesla.Middleware.JSON]

  @adapter Tesla.Adapter.Hackney

  def new() do
    middleware =
      [
        {Tesla.Middleware.Retry,
         delay: 500,
         max_retries: 10,
         max_delay: 4_000,
         should_retry: fn
           {:ok, %{status: status}} when status in [400, 500] -> true
           {:ok, _} -> false
           {:error, _} -> true
         end}
      ] ++ @middleware

    adapter = {@adapter, pool: __MODULE__, recv_timeout: 60_000}

    Tesla.client(middleware, adapter)
  end

  def post(client, source, rate, recent_events \\ []) do
    prepped_recent_events = prep_recent_events(recent_events)

    case Tesla.post(client, source.webhook_notification_url, %{
           rate: rate,
           source_name: source.name,
           source: source.token,
           recent_events: prepped_recent_events
         }) do
      {:ok, %Tesla.Env{status: 200} = response} ->
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

  defp prep_recent_events(recent_events) do
    Enum.take(recent_events, -5)
    |> Enum.map(fn x -> x.body.message end)
  end
end
