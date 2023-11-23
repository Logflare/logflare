defmodule Logflare.Telemetry do
  use Supervisor
  import Telemetry.Metrics

  def start_link(arg), do: Supervisor.start_link(__MODULE__, arg, name: __MODULE__)

  @caches [
    {Logflare.Logs.LogEvents.Cache, :log_events},
    {Logflare.Logs.RejectedLogEvents, :rejected_log_events},
    {Logflare.Sources.Cache, :sources},
    {Logflare.SourceSchemas.Cache, :source_schemas},
    {Logflare.PubSubRates.Cache, :pub_sub_rates},
    {Logflare.Billing.Cache, :billing},
    {Logflare.Users.Cache, :users}
  ]

  @impl true
  def init(_arg) do
    children = [
      {:telemetry_poller, measurements: periodic_measurements(), period: 10_000}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def metrics do
    cache_stats? = Application.get_env(:logflare, :cache_stats, false)

    cache_metrics =
      if cache_stats? do
        Enum.flat_map(@caches, fn {_cache, metric} ->
          [
            last_value("cachex.#{metric}.purge"),
            last_value("cachex.#{metric}.stats"),
            last_value("cachex.#{metric}.evictions"),
            last_value("cachex.#{metric}.expirations"),
            last_value("cachex.#{metric}.operations"),
            last_value("cachex.#{metric}.total_heap_size", unit: {:byte, :megabyte})
          ]
        end)
      else
        []
      end

    phoenix_metrics = [
      summary("phoenix.endpoint.stop.duration", unit: {:native, :millisecond}),
      summary("phoenix.router_dispatch.stop.duration", unit: {:native, :millisecond})
    ]

    database_metrics = [
      summary("logflare.repo.query.total_time", unit: {:native, :millisecond}),
      summary("logflare.repo.query.decode_time", unit: {:native, :millisecond}),
      summary("logflare.repo.query.query_time", unit: {:native, :millisecond}),
      summary("logflare.repo.query.queue_time", unit: {:native, :millisecond}),
      summary("logflare.repo.query.idle_time", unit: {:native, :millisecond})
    ]

    vm_metrics = [
      summary("vm.memory.total", unit: {:byte, :kilobyte}),
      summary("vm.total_run_queue_lengths.total"),
      summary("vm.total_run_queue_lengths.cpu"),
      summary("vm.total_run_queue_lengths.io")
    ]

    broadway_metrics = [
      summary("broadway.batcher.stop", unit: {:native, :millisecond}),
      summary("broadway.batch_processor.stop", unit: {:native, :millisecond}),
      summary("broadway.processor.message.stop", unit: {:native, :millisecond}),
      summary("broadway.processor.stop", unit: {:native, :millisecond})
    ]

    application_metrics = [
      summary("logflare.logs.processor.ingest.stop.duration",
        tags: [:processor],
        unit: {:native, :millisecond}
      ),
      counter("logflare.logs.processor.ingest.stop.duration", tags: [:processor]),
      sum("logflare.logs.processor.ingest.logs.count", tags: [:processor]),
      summary("logflare.logs.processor.ingest.store.stop.duration",
        tags: [:processor],
        unit: {:native, :millisecond}
      ),
      summary("logflare.logs.processor.ingest.handle_batch.stop.duration",
        tags: [:processor],
        unit: {:native, :millisecond}
      ),
      summary("logflare.ingest.pipeline.handle_batch.batch_size", tags: [:pipeline]),
      summary("logflare.ingest.common_pipeline.handle_batch.batch_size", tags: [:pipeline]),
      counter("logflare.context_cache.busted.count", tags: [:schema, :table]),
      counter("logflare.context_cache.handle_record.count", tags: [:schema, :table]),
      counter("logflare.logs.ingest_logs.drop"),
      counter("logflare.logs.ingest_logs.rejected"),
      counter("logflare.logs.ingest_logs.buffer_full")
    ]

    Enum.concat([
      phoenix_metrics,
      database_metrics,
      vm_metrics,
      cache_metrics,
      broadway_metrics,
      application_metrics
    ])
  end

  defp periodic_measurements() do
    cache_stats? = Application.get_env(:logflare, :cache_stats, false)

    if cache_stats? do
      [{__MODULE__, :cachex_metrics, []}]
    else
      []
    end
  end

  def cachex_metrics do
    Enum.each(@caches, fn {cache, metric} ->
      {:ok, stats} = Cachex.stats(cache)

      {:total_heap_size, total_heap_size} =
        cache
        |> Process.whereis()
        |> Process.info(:total_heap_size)

      metrics = %{
        purge: Map.get(stats, :purge),
        stats: Map.get(stats, :stats),
        evictions: Map.get(stats, :evictions),
        expirations: Map.get(stats, :expirations),
        operations: Map.get(stats, :operations),
        total_heap_size: total_heap_size
      }

      :telemetry.execute([:cachex, metric], metrics)
    end)
  end
end
