# Snapshot the pre-feedback transformer, then run without starting the application:
# jj file show -r 2dc251d0 lib/logflare/logs/ingest_transformers.ex > /tmp/byte-accounting-baseline.ex
# BYTE_ACCOUNTING_BASELINE=/tmp/byte-accounting-baseline.ex ../bin/x mix run --no-start bench/ingest_byte_accounting.exs

alias Logflare.LogEvent
alias Logflare.Logs.IngestTransformers

baseline_path = System.fetch_env!("BYTE_ACCOUNTING_BASELINE")

baseline_path
|> File.read!()
|> String.replace(
  "defmodule Logflare.Logs.IngestTransformers do",
  "defmodule ByteAccountingBaseline do"
)
|> Code.compile_string()

inputs = %{
  "small log" => %{
    "event_message" => "request completed",
    "timestamp" => 1_790_000_000_000_000,
    "metadata" => %{"status" => 200, "cached" => false}
  },
  "key-heavy edge log (synthetic)" => %{
    "event_message" => "GET /rest/v1/example",
    "timestamp" => 1_790_000_000_000_000,
    "metadata" =>
      Map.new(1..80, fn n ->
        {"CloudflareRequestAttribute_#{n}", if(rem(n, 3) == 0, do: n, else: "value-#{n}")}
      end)
  },
  "nested log" => %{
    "metadata" =>
      Enum.map(1..20, fn n ->
        %{"request" => %{"status-code" => 200, "path" => "/path/#{n}", "empty" => nil}}
      end)
  },
  "numeric histogram" => %{
    "metric_name" => "request_latency",
    "bucket_counts" => Enum.to_list(1..1024),
    "explicit_bounds" => Enum.map(1..1024, &(&1 / 10))
  }
}

baseline = fn input -> ByteAccountingBaseline.transform(input, :clean_to_bigquery_column_spec) end

for {name, input} <- inputs do
  body = baseline.(input)
  {fused_body, accounted_bytes} = IngestTransformers.transform_with_byte_size(input)
  true = body == fused_body
  true = accounted_bytes == LogEvent.body_byte_size(body)
  IO.puts("#{name}: batch=#{:erlang.external_size(body)} accounted=#{accounted_bytes}")
end

time = System.get_env("BYTE_ACCOUNTING_BENCH_TIME", "2") |> String.to_integer()

Benchee.run(
  %{
    "existing transform + batch estimate" => fn input ->
      body = baseline.(input)
      {body, :erlang.external_size(body)}
    end,
    "separate accounting + batch estimate" => fn input ->
      body = baseline.(input)
      {body, :erlang.external_size(body), LogEvent.body_byte_size(body)}
    end,
    "fused accounting + batch estimate" => fn input ->
      {body, bytes} = IngestTransformers.transform_with_byte_size(input)
      {body, :erlang.external_size(body), bytes}
    end,
    "separate accounting + three queue calculations" => fn input ->
      body = baseline.(input)
      for _ <- 1..3, do: {:erlang.external_size(body), LogEvent.body_byte_size(body)}
    end,
    "fused accounting + three cached queue reads" => fn input ->
      {body, bytes} = IngestTransformers.transform_with_byte_size(input)
      event = LogEvent.cache_sizes(%LogEvent{body: body, accounted_bytes: bytes})
      for _ <- 1..3, do: {LogEvent.batch_byte_size(event), LogEvent.accounted_byte_size(event)}
    end
  },
  inputs: inputs,
  time: time,
  warmup: 1,
  memory_time: 1,
  reduction_time: 1,
  parallel: 1,
  print: [configuration: true, benchmarking: false],
  formatters: [{Benchee.Formatters.Console, extended_statistics: false}]
)
