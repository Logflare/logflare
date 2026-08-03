# Usage: mix run --no-start bench/clickhouse_encoder_primitives.exs
#
# Exploratory micro-benchmarks for candidate RowBinaryEncoder optimizations
# identified while reviewing encode_batch. Each candidate is compared against
# the current production implementation, replicated locally as the baseline,
# with a byte-equality assertion first.

alias Logflare.Backends.Adaptor.ClickHouseAdaptor.RowBinaryEncoder

varint = &RowBinaryEncoder.varint/1
string = fn v when is_binary(v) -> [varint.(byte_size(v)), v] end

# ---------------------------------------------------------------------------
# Candidate 1: empty-array / empty-map sentinel  [<<0>>]  vs  <<0>>
#
# A metric/trace row contains many optional array columns that are empty in
# practice. The current code returns a fresh single-element list [<<0>>] each
# time; a shared <<0>> binary literal allocates nothing. Measured by building
# a row-shaped iolist with N empty arrays interspersed with real values.
# ---------------------------------------------------------------------------

empty_list = fn -> [<<0>>] end
empty_bin = fn -> <<0>> end

# Simulate the empty-array slots of a metric row (~10 empties is realistic for
# a gauge/sum metric where histogram buckets/quantiles/exemplars are unused).
build_row_list = fn ->
  for _ <- 1..10, do: empty_list.()
end

build_row_bin = fn ->
  for _ <- 1..10, do: empty_bin.()
end

IO.puts("== Candidate 1: empty sentinel [<<0>>] vs <<0>> (10 empty arrays/row) ==")

l = IO.iodata_to_binary(build_row_list.())
b = IO.iodata_to_binary(build_row_bin.())
IO.puts(if l == b, do: "outputs match (#{byte_size(l)} bytes)", else: "MISMATCH")

Benchee.run(
  %{
    "[<<0>>] list sentinel" => build_row_list,
    "<<0>> binary sentinel" => build_row_bin
  },
  time: 2,
  warmup: 1,
  memory_time: 1,
  print: [configuration: false]
)

# ---------------------------------------------------------------------------
# Candidate 2: map_string_string fold variants
#
#   baseline  : :maps.fold building [acc, string(k), string(v)]  (current)
#   inlined   : :maps.fold building [acc, varint(byte_size(k)), k, varint(byte_size(v)), v]
#               -- drops the two intermediate [varint, str] lists from string/1
# ---------------------------------------------------------------------------

baseline_map = fn items ->
  :maps.fold(
    fn k, v, acc -> [acc, string.(k), string.(v)] end,
    [varint.(map_size(items))],
    items
  )
end

inlined_map = fn items ->
  :maps.fold(
    fn k, v, acc ->
      [acc, varint.(byte_size(k)), k, varint.(byte_size(v)), v]
    end,
    [varint.(map_size(items))],
    items
  )
end

# Guarded: fast path for binary k/v, falls back to string/1 for iodata values
# (preserves the current map/3 capability of accepting iodata-list values).
guarded_map = fn items ->
  fun = fn
    k, v, acc when is_binary(k) and is_binary(v) ->
      [acc, varint.(byte_size(k)), k, varint.(byte_size(v)), v]

    k, v, acc ->
      [acc, string.(k), string.(v)]
  end

  :maps.fold(fun, [varint.(map_size(items))], items)
end

small = %{"host" => "node-3", "region" => "us-east-1", "status" => "200"}

medium =
  for i <- 1..15, into: %{}, do: {"attribute_key_#{i}", "some attribute value number #{i}"}

large =
  for i <- 1..100, into: %{}, do: {"attribute_key_#{i}", "some attribute value number #{i}"}

for {label, m} <- [{"small (3 keys)", small}, {"medium (15 keys)", medium}, {"large (100 keys)", large}] do
  IO.puts("\n== Candidate 2: map_string_string fold -- #{label} ==")

  a = IO.iodata_to_binary(baseline_map.(m))
  c = IO.iodata_to_binary(inlined_map.(m))
  g = IO.iodata_to_binary(guarded_map.(m))
  IO.puts(if a == c and c == g, do: "outputs match (#{byte_size(a)} bytes)", else: "MISMATCH")

  Benchee.run(
    %{
      "baseline (string/1 per k,v)" => fn -> baseline_map.(m) end,
      "inlined (byte_size in fold)" => fn -> inlined_map.(m) end,
      "guarded (binary fast path)" => fn -> guarded_map.(m) end
    },
    time: 2,
    warmup: 1,
    memory_time: 1,
    print: [configuration: false]
  )
end
