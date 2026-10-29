# Usage: MIX_ENV=test mix run test/profiling/ingest_transformers_bench.exs
#
# Measures IngestTransformers.transform_with_byte_size/1, which cleans and
# normalizes every ingested body, across mostly-clean and dirty payloads.

alias Logflare.Logs.IngestTransformers

clean = fn i ->
  %{
    "message" => "GET /api/v1/items/#{i} 200 in 12ms",
    "metadata" => %{
      "request" => %{
        "method" => "GET",
        "path" => "/api/v1/items/#{i}",
        "headers" => %{"user_agent" => "curl/8.0", "x_forwarded_for" => "1.2.3.4"}
      },
      "response" => %{"status" => 200, "duration_ms" => 12.5},
      "tags" => ["a", "b", "c"],
      "level" => "info",
      "context" => %{"pid" => "<0.123.0>", "module" => "Foo.Bar", "line" => i},
      "spans" => [%{"name" => "db", "ms" => 3}, %{"name" => "render", "ms" => 7}]
    }
  }
end

dirty = fn i ->
  i
  |> clean.()
  |> put_in(["metadata", "request", "headers"], %{"user-agent" => "curl/8.0", "x-trace" => nil})
  |> put_in(["metadata", "empty"], %{})
end

inputs = %{
  "clean body x250" => Enum.map(1..250, clean),
  "dirty body x250" => Enum.map(1..250, dirty)
}

Benchee.run(
  %{
    "transform_with_byte_size" => fn bodies ->
      Enum.each(bodies, &IngestTransformers.transform_with_byte_size/1)
    end
  },
  inputs: inputs,
  time: 5,
  warmup: 1,
  memory_time: 1,
  reduction_time: 1
)
