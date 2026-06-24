# HTTP ingest throughput benchmark using real-world event payloads.
#
# Runs as a standalone Elixir script against an already-running Logflare server.
# No app startup required — start Logflare separately first.
#
# Required env vars:
#   BENCH_API_KEY       — user API key (X-API-KEY header)
#
# Optional env vars:
#   BENCH_SOURCE        — source name (default: loadfest.test.0)
#   BENCH_EVENTS        — total events to send (default: 1_000_000)
#   BENCH_BATCH_SIZE    — events per HTTP POST (default: 250)
#   BENCH_CONCURRENCY   — concurrent in-flight requests (default: 20)
#   BENCH_HOST          — server base URL (default: http://localhost:4000)
#   INGEST_EXAMPLES_DIR — path to JSON example files
#
# Usage:
#   BENCH_API_KEY=my-other-cool-api-key-123 elixir scripts/http_bench.exs

Application.ensure_all_started(:inets)
Application.ensure_all_started(:ssl)

api_key = System.get_env("BENCH_API_KEY") || (IO.puts("ERROR: BENCH_API_KEY not set"); System.halt(1))
source_name = System.get_env("BENCH_SOURCE", "loadfest.test.0")
target = String.to_integer(System.get_env("BENCH_EVENTS", "1000000"))
batch_size = String.to_integer(System.get_env("BENCH_BATCH_SIZE", "250"))
concurrency = String.to_integer(System.get_env("BENCH_CONCURRENCY", "20"))
host = System.get_env("BENCH_HOST", "http://localhost:4000")

examples_dir =
  System.get_env(
    "INGEST_EXAMPLES_DIR",
    "/Users/brian/supabase/logflare-sql/ingest_examples"
  )

url = ~c"#{host}/logs?source_name=#{URI.encode(source_name)}"
headers = [{~c"x-api-key", ~c"#{api_key}"}]

# Load templates sorted alphabetically — identical order on every run.
templates =
  Path.wildcard(Path.join(examples_dir, "*.json"))
  |> Enum.sort()
  |> Enum.map(fn path -> path |> File.read!() |> :json.decode() end)

if templates == [] do
  IO.puts("ERROR: no JSON files found in #{examples_dir}")
  System.halt(1)
end

IO.puts("Loaded #{length(templates)} event templates")
IO.puts("Target: #{target} events → #{host}/logs?source_name=#{source_name}")
IO.puts("Building batches...")

now_us = System.os_time(:microsecond)

iso_to_ns = fn str ->
  {:ok, dt, _} = DateTime.from_iso8601(str)
  DateTime.to_unix(dt, :nanosecond)
end

batches =
  Stream.cycle(templates)
  |> Stream.take(target)
  |> Stream.with_index()
  |> Stream.map(fn {template, i} ->
    template
    |> Map.delete("id")
    |> Map.put("timestamp", now_us + i)
    |> then(fn ev ->
      ev
      |> (fn e ->
            if is_binary(Map.get(e, "start_time")),
              do: Map.update!(e, "start_time", iso_to_ns),
              else: e
          end).()
      |> (fn e ->
            if is_binary(Map.get(e, "end_time")),
              do: Map.update!(e, "end_time", iso_to_ns),
              else: e
          end).()
    end)
  end)
  |> Stream.chunk_every(batch_size)
  |> Enum.to_list()

IO.puts("#{length(batches)} batches of up to #{batch_size} events | concurrency=#{concurrency}\n")

counter = :atomics.new(1, [])
errors = :atomics.new(1, [])

send_batch = fn batch ->
  body = :json.encode(%{"batch" => batch})

  Stream.repeatedly(fn -> nil end)
  |> Enum.reduce_while(:ok, fn _, _ ->
    case :httpc.request(:post, {url, headers, ~c"application/json", body}, [{:timeout, 30_000}], []) do
      {:ok, {{_, status, _}, _resp_headers, _resp_body}} when status in 200..299 ->
        :atomics.add(counter, 1, length(batch))
        {:halt, :ok}

      {:ok, {{_, 429, _}, _resp_headers, _resp_body}} ->
        Process.sleep(1_000)
        {:cont, :ok}

      {:ok, {{_, status, _}, _resp_headers, resp_body}} ->
        IO.puts("\nHTTP #{status}: #{resp_body}")
        :atomics.add(errors, 1, 1)
        {:halt, :error}

      {:error, reason} ->
        IO.puts("\nRequest error: #{inspect(reason)}")
        :atomics.add(errors, 1, 1)
        {:halt, :error}
    end
  end)
end

start_ms = System.monotonic_time(:millisecond)

Task.async_stream(batches, send_batch, max_concurrency: concurrency, timeout: 120_000, ordered: false)
|> Stream.each(fn _ ->
  total = :atomics.get(counter, 1)
  elapsed_ms = System.monotonic_time(:millisecond) - start_ms
  elapsed_s = max(elapsed_ms, 1) / 1_000
  rate = round(total / elapsed_s)
  IO.write("\r  sent=#{total}/#{target} (#{round(total / max(target, 1) * 100)}%) #{rate}/s")
end)
|> Stream.run()

elapsed_s = (System.monotonic_time(:millisecond) - start_ms) / 1_000
total_sent = :atomics.get(counter, 1)
total_errors = :atomics.get(errors, 1)
avg_rate = round(total_sent / max(elapsed_s, 0.001))

IO.puts("\nDONE: #{total_sent}/#{target} events in #{Float.round(elapsed_s, 2)}s = #{avg_rate}/s avg (#{total_errors} errors)")
