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
    cache_metrics =
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

    phoenix_metrics ++ database_metrics ++ vm_metrics ++ cache_metrics ++ broadway_metrics
  end

  defp periodic_measurements() do
    [{__MODULE__, :cachex_metrics, []}]
  end

  def cachex_metrics do
    Enum.each(@caches, fn {cache, metric} ->
      {:ok, stats} = Cachex.stats(cache)

      process_info =
        cache
        |> Process.whereis()
        |> Process.info()

      metrics = %{
        purge: Map.get(stats, :purge),
        stats: Map.get(stats, :stats),
        evictions: Map.get(stats, :evictions),
        expirations: Map.get(stats, :expirations),
        operations: Map.get(stats, :operations),
        total_heap_size: Keyword.get(process_info, :total_heap_size)
      }

      :telemetry.execute([:cachex, metric], metrics)
    end)
  end
end
