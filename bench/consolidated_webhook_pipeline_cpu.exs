# CPU and memory benchmark: the webhook batch path against the consolidated_webhook
# batch path.
#
# Both scenarios run in one process, from the queue insert to the gzip request body.
# There is no network I/O, so the result shows local CPU, ETS, JSON, and gzip cost only.
#
#   webhook:             add_to_table -> pop_pending (full LogEvents) -> format_payload ->
#                        Jason.encode! of the whole batch -> gzip
#   consolidated_webhook: add_to_table -> pop_pending_pointers ->
#                        Pipeline.handle_message for each event
#                        (ETS lookup, Jason.encode, replace_event) -> join_payload ->
#                        gzip -> ack
#
# The real consolidated_webhook pipeline runs handle_message on 2 processors in
# parallel. This script does not. Multiply ips by the batch size for events per second.
#
# Usage:
#
#   mix run --no-start bench/consolidated_webhook_pipeline_cpu.exs
#   BATCH_SIZES=250,1000 SHAPES=realistic FORMATS=json,ndjson BENCH_TIME=5 \
#     mix run --no-start bench/consolidated_webhook_pipeline_cpu.exs

Code.require_file("support/clickhouse_pipeline_bench_data.exs", __DIR__)

alias Broadway.Message
alias Logflare.Backends.Adaptor.WebhookAdaptor
alias Logflare.Backends.Adaptor.ConsolidatedWebhookAdaptor.EncodedEvent
alias Logflare.Backends.Adaptor.ConsolidatedWebhookAdaptor.Pipeline
alias Logflare.Backends.IngestEventQueue
alias Logflare.Bench.ClickHousePipelineData

parse_csv = fn env, default, mapper ->
  env
  |> System.get_env(default)
  |> String.split(",", trim: true)
  |> Enum.map(mapper)
end

batch_sizes = parse_csv.("BATCH_SIZES", "250,1000,10000", &String.to_integer/1)
shapes = parse_csv.("SHAPES", "realistic", &String.to_existing_atom/1)
formats = parse_csv.("FORMATS", "json", & &1)
benchee_time = System.get_env("BENCH_TIME", "5") |> String.to_integer()
benchee_warmup = System.get_env("BENCH_WARMUP", "2") |> String.to_integer()

webhook_batch = fn %{events: events, queue_key: key, queue_tid: tid} ->
  :ets.delete_all_objects(tid)
  :ok = IngestEventQueue.add_to_table({key, tid}, events)
  {:ok, popped} = IngestEventQueue.pop_pending(key, length(events))

  %{}
  |> WebhookAdaptor.format_payload(popped)
  |> Jason.encode!()
  |> :zlib.gzip()
end

consolidated_batch = fn %{
                          events: events,
                          queue_key: key,
                          queue_tid: tid,
                          backend_id: backend_id,
                          config: config
                        } ->
  :ets.delete_all_objects(tid)
  :ok = IngestEventQueue.add_to_table({key, tid}, events)
  {:ok, pointers, _tid} = IngestEventQueue.pop_pending_pointers(key, length(events))

  messages =
    Enum.map(pointers, fn pointer ->
      message = %Message{
        data: pointer,
        acknowledger: {Pipeline, :ack_id, %{backend_id: backend_id, in_flight_ref: nil}}
      }

      Pipeline.handle_message(:default, message, %{})
    end)

  encoded_events = for %Message{data: %EncodedEvent{json: json}} <- messages, do: json
  body = config |> Pipeline.join_payload(encoded_events) |> :zlib.gzip()
  :ok = Pipeline.ack(:ack_ref, messages, [])
  body
end

decode = fn
  body, "ndjson" -> body |> :zlib.gunzip() |> String.split("\n") |> Enum.map(&Jason.decode!/1)
  body, _format -> body |> :zlib.gunzip() |> Jason.decode!()
end

inputs =
  for {shape, shape_idx} <- Enum.with_index(shapes),
      {format, format_idx} <- Enum.with_index(formats),
      {batch_size, size_idx} <- Enum.with_index(batch_sizes),
      into: %{} do
    backend_id = 9_100_000 + shape_idx * 10_000 + format_idx * 100 + size_idx
    {queue_key, queue_tid} = ClickHousePipelineData.setup_queue(backend_id)

    input = %{
      events: ClickHousePipelineData.batch(:log, batch_size, shape),
      queue_key: queue_key,
      queue_tid: queue_tid,
      backend_id: backend_id,
      config: %{format: format}
    }

    consolidated_body = consolidated_batch.(input)
    consolidated_events = decode.(consolidated_body, format)

    if format == "json" and
         Enum.sort(decode.(webhook_batch.(input), format)) != Enum.sort(consolidated_events) do
      raise "webhook and consolidated_webhook payloads differ for batch=#{batch_size}"
    end

    if length(consolidated_events) != batch_size do
      raise "consolidated_webhook payload has #{length(consolidated_events)} events, expected #{batch_size}"
    end

    label = "#{shape}/#{format}/batch=#{batch_size}"

    IO.puts(
      "#{label}: #{byte_size(consolidated_body)} gzip bytes, #{byte_size(:zlib.gunzip(consolidated_body))} raw bytes"
    )

    {label, input}
  end

IO.puts("")

scenarios =
  if "json" in formats do
    %{"webhook: pop events + encode batch + gzip" => webhook_batch}
  else
    %{}
  end

Benchee.run(
  Map.put(
    scenarios,
    "consolidated webhook: pop pointers + encode each + join + gzip + ack",
    consolidated_batch
  ),
  inputs: inputs,
  time: benchee_time,
  warmup: benchee_warmup,
  memory_time: 1,
  print: [fast_warning: false]
)
