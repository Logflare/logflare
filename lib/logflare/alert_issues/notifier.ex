defmodule Logflare.AlertIssues.Notifier do
  @moduledoc """
  Sends alert issue notification events to the control plane, which fans them out to
  receivers. The control plane is the platform component on the hosted platform, or
  Studio's API route when self-hosted.

  Each request is signed: `X-Logflare-Signature: v1=<hex HMAC-SHA256(secret,
  "<timestamp>.<raw body>")>`, with the timestamp in `X-Logflare-Timestamp`.
  The URL is operator configuration (`LOGFLARE_ALERT_ISSUE_NOTIFIER_URL`), so it is
  not subject to the SSRF checks applied to user-supplied webhook URLs.
  """

  @receive_timeout 5_000

  @spec enabled?() :: boolean()
  def enabled?, do: is_binary(config(:url)) and config(:url) != ""

  @spec sign(String.t(), integer(), String.t()) :: String.t()
  def sign(secret, timestamp, raw_body) do
    "v1=" <>
      (:crypto.mac(:hmac, :sha256, secret, "#{timestamp}.#{raw_body}")
       |> Base.encode16(case: :lower))
  end

  @spec deliver(map()) :: {:ok, pos_integer()} | {:error, term()}
  def deliver(%{"event_id" => event_id} = event) do
    body = Jason.encode!(event)
    timestamp = System.system_time(:second)

    headers = [
      {"content-type", "application/json"},
      {"x-logflare-event-id", event_id},
      {"x-logflare-timestamp", Integer.to_string(timestamp)},
      {"x-logflare-signature", sign(config(:secret) || "", timestamp, body)}
    ]

    client =
      Tesla.client(
        [],
        {Tesla.Adapter.Finch, name: Logflare.FinchDefaultHttp1, receive_timeout: @receive_timeout}
      )

    case Tesla.post(client, config(:url), body, headers: headers) do
      {:ok, %Tesla.Env{status: status}} when status in 200..299 -> {:ok, status}
      {:ok, %Tesla.Env{status: status}} -> {:error, {:http_status, status}}
      {:error, _reason} = error -> error
    end
  end

  defp config(key), do: :logflare |> Application.get_env(__MODULE__, []) |> Keyword.get(key)
end
