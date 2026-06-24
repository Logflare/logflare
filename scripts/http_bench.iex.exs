# HTTP ingest throughput benchmark using real-world event payloads.
#
# Loads event templates from INGEST_EXAMPLES_DIR, cycles through them
# deterministically to build N events with current timestamps, then fires
# them at the local HTTP endpoint in concurrent batches. Measures events/s
# from handle_batch telemetry — same metric as bench.iex.exs.
#
# Usage:
#   BENCH_SOURCE=loadfest.test.0 BENCH_EVENTS=1000000 iex -S mix run scripts/http_bench.iex.exs

alias Logflare.Sources
alias Logflare.Users

source_name = System.get_env("BENCH_SOURCE", "loadfest.test.0")
target = String.to_integer(System.get_env("BENCH_EVENTS", "1000000"))
batch_size = String.to_integer(System.get_env("BENCH_BATCH_SIZE", "250"))
concurrency = String.to_integer(System.get_env("BENCH_CONCURRENCY", "20"))

examples_dir =
  System.get_env(
    "INGEST_EXAMPLES_DIR",
    "/Users/brian/supabase/logflare-sql/ingest_examples"
  )

source = Sources.get_by(name: source_name)

unless source do
  IO.puts("ERROR: source #{inspect(source_name)} not found")
  System.halt(1)
end

user = Users.get_by(id: source.user_id)

unless user && user.api_key do
  IO.puts("ERROR: could not load api_key for source owner")
  System.halt(1)
end

source_token = to_string(source.token)
api_key = user.api_key
url = "http://localhost:4000/api/logs?source=#{source_token}&api_key=#{api_key}"

# Load templates sorted alphabetically — identical order on every run.
templates =
  Path.wildcard(Path.join(examples_dir, "*.json"))
  |> Enum.sort()
  |> Enum.map(fn path ->
    path |> File.read!() |> Jason.decode!()
  end)

unless templates != [] do
  IO.puts("ERROR: no JSON files found in #{examples_dir}")
  System.halt(1)
end

IO.puts("Loaded #{length(templates)} event templates from #{examples_dir}")
IO.puts("Building #{target} events...")

now_us = System.os_time(:microsecond)

batches =
  Stream.cycle(templates)
  |> Stream.take(target)
  |> Stream.with_index()
  |> Stream.map(fn {template, i} ->
    template
    |> Map.delete("id")
    |> Map.put("timestamp", now_us + i)
  end)
  |> Stream.chunk_every(batch_size)
  |> Enum.to_list()

IO.puts("Built #{length(batches)} batches of up to #{batch_size} events each")

counter = :atomics.new(1, [])

:telemetry.detach("http-bench-handle-batch")

:ok =
  :telemetry.attach(
    "http-bench-handle-batch",
    [:logflare, :backends, :pipeline, :handle_batch],
    fn _event, %{batch_size: size}, _meta, ref -> :atomics.add(ref, 1, size) end,
    counter
  )

headers = [{"content-type", "application/json"}]

send_batch = fn batch ->
  body = Jason.encode!(%{"batch" => batch})
  req = Finch.build(:post, url, headers, body)

  Stream.repeatedly(fn -> nil end)
  |> Enum.reduce_while(:ok, fn _, _ ->
    case Finch.request(req, Logflare.FinchDefault) do
      {:ok, %{status: status}} when status in 200..299 ->
        {:halt, :ok}

      {:ok, %{status: 429}} ->
        Process.sleep(1_000)
        {:cont, :ok}

      {:ok, %{status: status, body: resp_body}} ->
        IO.puts("\nHTTP #{status}: #{resp_body}")
        {:halt, :error}

      {:error, reason} ->
        IO.puts("\nRequest error: #{inspect(reason)}")
        {:halt, :error}
    end
  end)
end

IO.puts("Sending #{length(batches)} batches (concurrency=#{concurrency})...\n")

start_ms = System.monotonic_time(:millisecond)
timeout_ms = 300_000

Task.async_stream(batches, send_batch, max_concurrency: concurrency, timeout: 60_000, ordered: false)
|> Stream.run()

IO.puts("All batches sent. Waiting for pipeline to drain...")

Stream.repeatedly(fn ->
  total = :atomics.get(counter, 1)
  elapsed_ms = System.monotonic_time(:millisecond) - start_ms

  cond do
    total >= target ->
      elapsed_s = elapsed_ms / 1_000
      avg_rate = round(target / elapsed_s)

      IO.puts(
        "\nDONE: #{target} events in #{Float.round(elapsed_s, 2)}s = #{avg_rate}/s avg"
      )

      :telemetry.detach("http-bench-handle-batch")
      System.halt(0)

    elapsed_ms >= timeout_ms ->
      IO.puts("\nTIMEOUT: only #{total}/#{target} events processed in #{timeout_ms / 1_000}s")
      :telemetry.detach("http-bench-handle-batch")
      System.halt(1)

    true ->
      IO.write("\r  processed=#{total}/#{target} (#{round(total / max(target, 1) * 100)}%)")
      Process.sleep(200)
  end
end)
|> Stream.run()
