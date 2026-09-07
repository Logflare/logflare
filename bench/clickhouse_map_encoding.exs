# Usage: mix run bench/clickhouse_map_encoding.exs
#
# Compares candidate implementations of the String->String Map encoder used
# for ClickHouse attribute columns (resource/scope/log attributes).

alias Logflare.Backends.Adaptor.ClickHouseAdaptor.RowBinaryEncoder

string = fn value when is_binary(value) -> [RowBinaryEncoder.varint(byte_size(value)), value] end

# Baseline: current source impl (Map.to_list + length + Enum.flat_map).
baseline = fn items ->
  pairs = Map.to_list(items)

  [
    RowBinaryEncoder.varint(length(pairs))
    | Enum.flat_map(pairs, fn {k, v} -> [string.(k), string.(v)] end)
  ]
end

# Candidate A: map_size + comprehension, nested iodata (no flat_map flatten).
cand_a = fn items ->
  [
    RowBinaryEncoder.varint(map_size(items))
    | for({k, v} <- items, do: [string.(k), string.(v)])
  ]
end

# Candidate B: :maps.fold, no to_list materialization.
cand_b = fn items ->
  :maps.fold(
    fn k, v, acc -> [acc, string.(k), string.(v)] end,
    [RowBinaryEncoder.varint(map_size(items))],
    items
  )
end

small = %{"host" => "node-3", "region" => "us-east-1", "status" => "200"}

medium =
  for i <- 1..15, into: %{}, do: {"attribute_key_#{i}", "some attribute value number #{i}"}

large =
  for i <- 1..100, into: %{}, do: {"attribute_key_#{i}", "some attribute value number #{i}"}

# Sanity: all three must produce identical bytes.
for {name, m} <- [small: small, medium: medium, large: large] do
  a = IO.iodata_to_binary(baseline.(m))
  b = IO.iodata_to_binary(cand_a.(m))
  c = IO.iodata_to_binary(cand_b.(m))

  if a == b and b == c do
    IO.puts("#{name}: outputs match (#{byte_size(a)} bytes)")
  else
    IO.puts("#{name}: MISMATCH baseline=#{byte_size(a)} A=#{byte_size(b)} B=#{byte_size(c)}")
  end
end

for {label, m} <- [{"small (3 keys)", small}, {"medium (15 keys)", medium}, {"large (100 keys)", large}] do
  IO.puts("\n== #{label} ==")

  Benchee.run(
    %{
      "baseline (to_list+length+flat_map)" => fn -> baseline.(m) end,
      "A: map_size + comprehension" => fn -> cand_a.(m) end,
      "B: :maps.fold" => fn -> cand_b.(m) end
    },
    time: 2,
    warmup: 1,
    memory_time: 1,
    print: [configuration: false]
  )
end
