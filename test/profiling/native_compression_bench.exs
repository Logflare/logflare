alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.BlockEncoder
alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Compression

# ── Negotiated protocol revision (matches local Docker CH 25.12) ────────
negotiated_rev = 54_483

# ── Build normalized columns matching the otel_logs table schema ────────
#
# These types are post-normalization (LowCardinality stripped, Map keys
# normalized) matching what BlockEncoder actually receives.

build_columns = fn num_rows ->
  [
    {"id", "UUID", Enum.map(1..num_rows, fn _ -> :crypto.strong_rand_bytes(16) end)},
    {"source_uuid", "String", List.duplicate("a1b2c3d4-e5f6-7890-abcd-ef1234567890", num_rows)},
    {"source_name", "String", List.duplicate("edge-logs", num_rows)},
    {"project", "String", List.duplicate("zzzenjkohrkaatgpywnz", num_rows)},
    {"trace_id", "String",
     Enum.map(1..num_rows, fn _ -> Base.encode16(:crypto.strong_rand_bytes(16), case: :lower) end)},
    {"span_id", "String",
     Enum.map(1..num_rows, fn _ -> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower) end)},
    {"trace_flags", "UInt8", List.duplicate(0, num_rows)},
    {"severity_text", "String", List.duplicate("INFO", num_rows)},
    {"severity_number", "UInt8", List.duplicate(9, num_rows)},
    {"service_name", "String", List.duplicate("supabase-api-gateway", num_rows)},
    {"event_message", "String",
     Enum.map(1..num_rows, fn i ->
       "POST | #{Enum.random([200, 201, 404, 500])} | #{:rand.uniform(255)}.#{:rand.uniform(255)}.#{:rand.uniform(255)}.#{:rand.uniform(255)} | #{Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)} | https://zzzenjkohrkaatgpywnz.supabase.co/rest/v1/rpc/set_active_session_#{i}"
     end)},
    {"scope_name", "String",
     List.duplicate(
       "go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin",
       num_rows
     )},
    {"scope_version", "String", List.duplicate("0.61.0", num_rows)},
    {"scope_schema_url", "String", List.duplicate("", num_rows)},
    {"resource_schema_url", "String", List.duplicate("", num_rows)},
    {"resource_attributes", "Map(String, String)",
     List.duplicate(
       %{
         "cloud.region" => "us-east-1",
         "deployment.environment" => "staging",
         "service.name" => "supabase-api-gateway",
         "service.version" => "1.0.0"
       },
       num_rows
     )},
    {"scope_attributes", "Map(String, String)", List.duplicate(%{}, num_rows)},
    {"log_attributes", "Map(String, String)",
     Enum.map(1..num_rows, fn _ ->
       %{
         "http.method" => "POST",
         "http.status_code" => "#{Enum.random([200, 201, 404, 500])}",
         "http.url" => "https://zzzenjkohrkaatgpywnz.supabase.co/rest/v1/rpc/set_active_session",
         "http.user_agent" => "Deno/2.1.4 (variant; SupabaseEdgeRuntime/1.69.25)",
         "net.peer.ip" =>
           "#{:rand.uniform(255)}.#{:rand.uniform(255)}.#{:rand.uniform(255)}.#{:rand.uniform(255)}",
         "cf.ray" => Base.encode16(:crypto.strong_rand_bytes(8), case: :lower) <> "-BOM",
         "cf.colo" => "BOM",
         "cf.country" => "IN",
         "cf.city" => "Mumbai",
         "cf.timezone" => "Asia/Kolkata",
         "cf.asOrganization" => "Amazon Technologies Inc.",
         "cf.asn" => "16509",
         "cf.httpProtocol" => "HTTP/2",
         "cf.tlsVersion" => "TLSv1.3",
         "sb.gateway_version" => "1",
         "sb.request_id" => Ecto.UUID.generate(),
         "response.origin_time" => "#{:rand.uniform(500)}",
         "response.status_code" => "#{Enum.random([200, 201, 404, 500])}"
       }
     end)},
    {"mapping_config_id", "UUID", List.duplicate(<<0::128>>, num_rows)},
    {"timestamp", "DateTime64(9)", List.duplicate(System.system_time(:nanosecond), num_rows)}
  ]
end

# ── Pre-build columns and encoded block bodies for each size ────────────
sizes = [1_000, 2_000, 5_000, 10_000, 50_000]

# credo:disable-for-lines:5
columns_by_size =
  Map.new(sizes, fn n ->
    IO.puts("Building #{n}-row column set...")
    {n, build_columns.(n)}
  end)

# credo:disable-for-lines:7
encoded_by_size =
  Map.new(sizes, fn n ->
    cols = columns_by_size[n]
    encoded = BlockEncoder.encode_block_body(cols, negotiated_rev)
    binary = IO.iodata_to_binary(encoded)
    IO.puts("  #{n} rows: #{div(byte_size(binary), 1024)} KB encoded")
    {n, encoded}
  end)

# credo:disable-for-next-line
IO.puts("")

# ── Benchmark: Compression.compress/1 on pre-encoded blocks ────────────
# Measures the per-call NIF duration at various block sizes.
# The sub-block size in NativeIngester determines which row of this table
# represents the worst-case scheduler hold time in production.
# credo:disable-for-next-line
IO.puts("=== Benchmark: Compression.compress per block size ===\n")

Benchee.run(
  Map.new(sizes, fn n ->
    {"compress #{n} rows", fn -> Compression.compress(encoded_by_size[n]) end}
  end),
  time: 5,
  warmup: 2,
  memory_time: 3,
  reduction_time: 3
)

# Results — Compression.compress on pre-encoded otel_logs blocks
# Apple M4 / 32 GB / macOS / Elixir 1.19.5 / Erlang 27.3.4.6
#
# Encoded sizes: 1K=1033 KB, 2K=2068 KB, 5K=5172 KB, 10K=10345 KB, 50K=51766 KB
#
# Name                          ips        average  deviation         median         99th %
# compress 1000 rows         371.85        2.69 ms   ±842.12%        1.56 ms        4.74 ms
# compress 2000 rows          72.25       13.84 ms   ±374.28%        8.57 ms      419.83 ms
# compress 5000 rows          14.72       67.96 ms   ±167.63%       43.40 ms      594.36 ms
# compress 10000 rows          8.45      118.32 ms   ±123.96%       75.47 ms      626.96 ms
# compress 50000 rows          3.30      303.14 ms    ±47.17%      266.15 ms      819.81 ms
#
# Memory usage statistics:
#
# Name                   Memory usage
# compress 1000 rows            336 B
# compress 2000 rows            336 B
# compress 5000 rows            336 B
# compress 10000 rows           336 B
# compress 50000 rows           336 B
#
# Reduction count statistics:
#
# Name                        average  deviation         median         99th %
# compress 1000 rows          17.81 K     ±0.00%        17.81 K        17.81 K
# compress 2000 rows          35.57 K     ±0.00%        35.57 K        35.57 K
# compress 5000 rows          87.99 K     ±0.00%        87.99 K        87.99 K
# compress 10000 rows        175.99 K     ±0.00%       175.99 K       175.99 K
# compress 50000 rows       1850.55 K     ±0.06%      1850.78 K      1851.61 K
#
# With @sub_block_size=1_000, the worst-case per-NIF-call is ~1.56 ms median
# (the 1K row line above). The @max_block_bytes safety valve (1.5 MB) further
# splits any sub-block that exceeds the byte budget due to oversized events.
