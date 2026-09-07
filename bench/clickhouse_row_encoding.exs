# Usage: mix run bench/clickhouse_row_encoding.exs
#
# Measures two row-encoding optimizations for the ClickHouse ingester:
#   #1 hoisting the batch-constant mapping_config_id UUID encode out of the
#      per-row loop (encode_batch vs per-row encode_row)
#   #2 the binary-pattern-match fast path in RowBinaryEncoder.uuid/1 vs the
#      old String.replace |> Base.decode16! approach.

alias Logflare.Backends.Adaptor.ClickHouseAdaptor.Ingester
alias Logflare.Backends.Adaptor.ClickHouseAdaptor.RowBinaryEncoder
alias Logflare.LogEvent

mapping_config_id = "00000000-0000-0000-0001-000000000003"
source_uuid = "550e8400-e29b-41d4-a716-446655440000"

# Old uuid/1 implementation, replicated locally as the #2 baseline.
old_uuid = fn uuid_string ->
  <<u1::64, u2::64>> =
    uuid_string
    |> String.replace("-", "")
    |> Base.decode16!(case: :mixed)

  <<u1::64-little, u2::64-little>>
end

build_event = fn i ->
  %LogEvent{
    id: Ecto.UUID.generate(),
    source_uuid: String.to_atom(source_uuid),
    source_name: "bench source",
    event_type: :log,
    ingested_at: DateTime.utc_now(),
    body: %{
      "mapping_config_id" => mapping_config_id,
      "project" => "bench-project",
      "trace_id" => "abc123",
      "span_id" => "def456",
      "trace_flags" => 1,
      "severity_text" => "INFO",
      "severity_number" => 9,
      "service_name" => "bench-service",
      "event_message" => "an example log line number #{i}",
      "scope_name" => "bench.scope",
      "scope_version" => "1.0.0",
      "scope_schema_url" => "",
      "resource_schema_url" => "",
      "resource_attributes" => %{"host" => "node-#{rem(i, 8)}", "region" => "us-east-1"},
      "scope_attributes" => %{"lib" => "logflare"},
      "log_attributes" => %{"user_id" => "#{i}", "path" => "/api/v1/resource", "status" => "200"},
      "timestamp" => 1_700_000_000_000_000 + i
    }
  }
end

batch = Enum.map(1..500, build_event)

IO.puts("Batch size: #{length(batch)} log events\n")

IO.puts("== #1: batch encode (hoisted) vs per-row encode (re-parses uuid per row) ==")

Benchee.run(
  %{
    "per-row encode_row/2 (baseline)" =>
      fn -> Enum.map(batch, &Ingester.encode_row(&1, :log)) end,
    "encode_batch/2 (hoisted uuid)" =>
      fn -> Ingester.encode_batch(batch, :log) end
  },
  time: 3,
  warmup: 1,
  memory_time: 1
)

IO.puts("\n== #2: uuid/1 fast path vs old String.replace approach ==")

Benchee.run(
  %{
    "old uuid (String.replace)" => fn -> old_uuid.(mapping_config_id) end,
    "new uuid (binary match)" => fn -> RowBinaryEncoder.uuid(mapping_config_id) end
  },
  time: 3,
  warmup: 1,
  memory_time: 1
)
