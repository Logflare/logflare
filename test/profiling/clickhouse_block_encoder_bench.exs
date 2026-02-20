alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.BlockEncoder
alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Protocol

rev = Protocol.dbms_tcp_protocol_version()

# ---------------------------------------------------------------------------
# Data generators
# ---------------------------------------------------------------------------

gen_uint64 = fn n -> Enum.map(1..n, fn i -> i * 1_000_000 end) end

gen_string = fn n ->
  Enum.map(1..n, fn i -> "event_message_#{i}_" <> String.duplicate("x", 80) end)
end

gen_float64 = fn n -> Enum.map(1..n, fn i -> i * 1.5 end) end
gen_datetime = fn n -> Enum.map(1..n, fn i -> 1_700_000_000 + i end) end

gen_uuid = fn n ->
  Enum.map(1..n, fn _ ->
    hex = Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)

    <<a::binary-size(8), b::binary-size(4), c::binary-size(4), d::binary-size(4),
      e::binary-size(12)>> = hex

    "#{a}-#{b}-#{c}-#{d}-#{e}"
  end)
end

# ---------------------------------------------------------------------------
# Scenarios — 5-column block matching typical OTEL table shape
# ---------------------------------------------------------------------------

batch_sizes = [100, 1_000, 10_000]

scenarios =
  for n <- batch_sizes, into: %{} do
    columns = [
      {"id", "UInt64", gen_uint64.(n)},
      {"timestamp", "DateTime", gen_datetime.(n)},
      {"message", "String", gen_string.(n)},
      {"value", "Float64", gen_float64.(n)},
      {"request_id", "UUID", gen_uuid.(n)}
    ]

    {"5-col block (#{n} rows)", fn -> BlockEncoder.encode_data_block(columns, rev) end}
  end

all_scenarios =
  Map.put(scenarios, "empty block", fn -> BlockEncoder.encode_empty_block(rev) end)

# credo:disable-for-lines:1
IO.puts("\nBlockEncoder Benchmark — rev #{rev}\n")

Benchee.run(
  all_scenarios,
  time: 3,
  warmup: 1,
  memory_time: 2
)
