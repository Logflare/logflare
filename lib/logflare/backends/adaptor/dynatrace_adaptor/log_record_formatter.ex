defmodule Logflare.Backends.Adaptor.DynatraceAdaptor.LogRecordFormatter do
  @moduledoc """
  A `Tesla.Middleware` encoding `Logflare.LogEvent`s as
  [Dynatrace log records](https://docs.dynatrace.com/docs/dynatrace-api/environment-api/log-monitoring-v2/post-ingest-logs).

  The log event body is kept as the record attributes, with the message under `content`,
  the timestamp as an ISO 8601 string under `timestamp`, and the Logflare source name
  under `log.source`. Encoding is left to `Tesla.Middleware.JSON`.
  """

  alias Logflare.LogEvent

  @behaviour Tesla.Middleware

  @impl Tesla.Middleware
  def call(env, next, _opts) do
    env
    |> Tesla.put_body(Enum.map(env.body, &build_record/1))
    |> Tesla.run(next)
  end

  @spec build_record(LogEvent.t()) :: map()
  defp build_record(%LogEvent{body: body} = log_event) do
    timestamp =
      body["timestamp"]
      |> DateTime.from_unix!(:microsecond)
      |> DateTime.to_iso8601()

    Map.merge(body, %{
      "content" => body["event_message"] || body["message"] || "",
      "timestamp" => timestamp,
      "log.source" => log_event.source_name
    })
  end
end
