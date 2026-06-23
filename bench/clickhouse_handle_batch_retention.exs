# Usage: mix run bench/clickhouse_handle_batch_retention.exs
#
# Demonstrates the retained-memory difference in handle_batch between:
#   OLD: build a separate `events` list of mapped LogEvents while the original
#        `messages` (original bodies) stay alive for acking -> ~2x bodies held
#        through the insert window.
#   NEW: rewrite messages in place so `events` and the returned `messages`
#        share the same mapped structs; original bodies become garbage before
#        the insert -> ~1x bodies held.
#
# Retained size is measured with :erts_debug.size/1, which counts shared
# sub-terms within a term once -- so it reflects what is actually live at the
# point insert_log_events runs.

alias Broadway.Message
alias Logflare.LogEvent

build_body = fn i ->
  %{
    "project" => "bench-project",
    "service_name" => "bench-service",
    "event_message" => "an example log line number #{i}",
    "severity_text" => "INFO",
    "resource_attributes" => %{"host" => "node-#{rem(i, 8)}", "region" => "us-east-1"},
    "scope_attributes" => %{"lib" => "logflare"},
    "log_attributes" => %{"user_id" => "#{i}", "path" => "/api/v1/resource", "status" => "200"},
    "timestamp" => 1_700_000_000_000_000 + i
  }
end

# Simulates Mapper.map output: a new map of similar shape/size.
map_body = fn body ->
  body
  |> Map.put("mapping_config_id", "00000000-0000-0000-0001-000000000003")
  |> Map.put("severity_number", 9)
end

build_messages = fn n ->
  Enum.map(1..n, fn i ->
    event = %LogEvent{
      id: Ecto.UUID.generate(),
      source_uuid: :"00000000-0000-0000-0000-000000000000",
      source_name: "bench source",
      event_type: :log,
      body: build_body.(i)
    }

    %Message{data: event, acknowledger: {__MODULE__, :ack_id, %{backend_id: 1}}}
  end)
end

old_strategy = fn messages ->
  events =
    Enum.map(messages, fn %{data: %LogEvent{} = event} ->
      %{event | body: map_body.(event.body)}
    end)

  # Both `messages` (original bodies) and `events` (mapped) are live here.
  {messages, events}
end

new_strategy = fn messages ->
  messages =
    Enum.map(messages, fn %{data: %LogEvent{} = event} = message ->
      %{message | data: %{event | body: map_body.(event.body)}}
    end)

  events = Enum.map(messages, fn %{data: event} -> event end)

  # `events` and `messages` share the same mapped structs; originals are gone.
  {messages, events}
end

words_to_mb = fn words -> Float.round(words * :erlang.system_info(:wordsize) / 1_048_576, 2) end

IO.puts("Retained term size at the insert point ({messages, events}):\n")
IO.puts(String.pad_trailing("batch size", 14) <> String.pad_trailing("OLD (MB)", 12) <> String.pad_trailing("NEW (MB)", 12) <> "reduction")

for n <- [1_000, 10_000, 60_000] do
  messages = build_messages.(n)

  old_words = :erts_debug.size(old_strategy.(messages))
  new_words = :erts_debug.size(new_strategy.(messages))

  old_mb = words_to_mb.(old_words)
  new_mb = words_to_mb.(new_words)
  reduction = Float.round((1 - new_words / old_words) * 100, 1)

  IO.puts(
    String.pad_trailing("#{n}", 14) <>
      String.pad_trailing("#{old_mb}", 12) <>
      String.pad_trailing("#{new_mb}", 12) <>
      "-#{reduction}%"
  )
end
