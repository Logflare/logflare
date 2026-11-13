# Usage: MIX_ENV=test mix run test/profiling/custom_event_message_bench.exs

alias Logflare.LogEvent
alias Logflare.Sources.Source

body = %{
  "id" => "abc",
  "timestamp" => 1_765_540_359_181_556,
  "event_message" => "GET /api/v1/resource 503",
  "metadata" => %{
    "level" => "error",
    "status" => 503,
    "user_id" => "user-123",
    "request" => %{"method" => "GET", "path" => "/api/v1/resource"}
  }
}

events = for i <- 1..250, do: %LogEvent{id: "id-#{i}", body: body, accounted_bytes: 100}

prepare = fn source ->
  if function_exported?(Source, :parse_custom_event_message_keys, 1),
    do: Source.parse_custom_event_message_keys(source),
    else: source
end

Benchee.run(
  %{
    "apply_custom_event_message x250" => fn source ->
      source = prepare.(source)
      Enum.each(events, &LogEvent.apply_custom_event_message(&1, source))
    end
  },
  inputs: %{
    "3 json paths + id + message" => %Source{
      custom_event_message_keys: "m.level, m.request.method, metadata.status, event_message, id"
    },
    "1 json path" => %Source{custom_event_message_keys: "m.user_id"}
  },
  time: 4,
  warmup: 1,
  memory_time: 1,
  reduction_time: 1
)
