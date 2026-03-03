defmodule Logflare.Telemetry do
  use Supervisor

  import Telemetry.Metrics
  import Logflare.Utils, only: [ets_info: 1]

  def start_link(arg), do: Supervisor.start_link(__MODULE__, arg, name: __MODULE__)

  context_caches_with_metrics = Logflare.ContextCache.Supervisor.list_caches_with_metrics()

  @caches [
            {Logflare.Logs.LogEvents.Cache, :log_events},
            {Logflare.Logs.RejectedLogEvents, :rejected_log_events},
            {Logflare.PubSubRates.Cache, :pub_sub_rates}
          ] ++ context_caches_with_metrics

  @process_metrics %{
    memory: %{
      process_attribute: :memory,
      measurement: :size
    },
    message_queue: %{
      process_attribute: :message_queue_len,
      measurement: :length
    }
  }

  @metrics_interval 30_000

  @impl true
  def init(_arg) do
    base = System.schedulers_online()

    otel_exporter =
      if Application.get_env(:logflare, :opentelemetry_enabled?) do
        otel_exporter_opts =
          Application.get_all_env(:opentelemetry_exporter)
          |> Keyword.put(:metrics, metrics())
          |> Keyword.put(:resource, %{
            name: "Logflare",
            service: %{
              name: "Logflare",
              version: Application.spec(:logflare, :vsn) |> to_string()
            },
            node: inspect(Node.self()),
            cluster: Application.get_env(:logflare, :metadata)[:cluster]
          })
          |> Keyword.update!(:otlp_headers, &Map.new/1)
          # set finch pool to 100 size
          |> Keyword.put(:otlp_concurrent_requests, max(base * 4, 50))

        [{OtelMetricExporter, otel_exporter_opts}]
      else
        []
      end

    children =
      [
        {:telemetry_poller, measurements: periodic_measurements(), period: @metrics_interval}
      ] ++ otel_exporter

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp metrics do
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
            last_value("cachex.#{metric}.hits"),
            last_value("cachex.#{metric}.misses"),
            last_value("cachex.#{metric}.hit_rate"),
            last_value("cachex.#{metric}.miss_rate"),
            last_value("cachex.#{metric}.total_heap_size", unit: {:byte, :megabyte})
          ]
        end)
      else
        []
      end

    phoenix_metrics = [
      distribution("phoenix.endpoint.stop.duration", unit: {:native, :millisecond}),
      distribution("phoenix.router_dispatch.stop.duration", unit: {:native, :millisecond})
    ]

    database_metrics = [
      distribution("logflare.repo.query.total_time", unit: {:native, :millisecond}),
      # TODO: decode_time is `nil` in some of the ecto queries
      # In most telemetry adapters this is fine, but it causes issues in OtelMetricExporter
      # distribution("logflare.repo.query.decode_time", unit: {:native, :millisecond}),
      distribution("logflare.repo.query.query_time", unit: {:native, :millisecond}),
      distribution("logflare.repo.query.queue_time", unit: {:native, :millisecond}),
      distribution("logflare.repo.query.idle_time", unit: {:native, :millisecond})
    ]

    vm_metrics = [
      last_value("vm.memory.total", unit: {:byte, :kilobyte}),
      last_value("vm.total_run_queue_lengths.total"),
      last_value("vm.total_run_queue_lengths.cpu"),
      last_value("vm.total_run_queue_lengths.io"),
      last_value("logflare.system.observer.metrics.uptime", unit: {:second, :millisecond}),
      last_value("logflare.system.observer.metrics.run_queue"),
      last_value("logflare.system.observer.metrics.io_input", unit: {:byte, :kilobyte}),
      last_value("logflare.system.observer.metrics.io_output", unit: {:byte, :kilobyte}),
      last_value("logflare.system.observer.metrics.logical_processors"),
      last_value("logflare.system.observer.metrics.logical_processors_online"),
      last_value("logflare.system.observer.metrics.logical_processors_available"),
      last_value("logflare.system.observer.metrics.schedulers"),
      last_value("logflare.system.observer.metrics.schedulers_online"),
      last_value("logflare.system.observer.metrics.otp_release"),
      last_value("logflare.system.observer.metrics.atom_limit"),
      last_value("logflare.system.observer.metrics.atom_count"),
      last_value("logflare.system.observer.metrics.process_limit"),
      last_value("logflare.system.observer.metrics.process_count"),
      last_value("logflare.system.observer.metrics.port_limit"),
      last_value("logflare.system.observer.metrics.port_count"),
      last_value("logflare.system.observer.metrics.ets_limit"),
      last_value("logflare.system.observer.metrics.ets_count"),
      last_value("logflare.system.observer.metrics.total_active_tasks"),
      last_value("logflare.system.observer.memory.total", unit: {:byte, :kilobyte}),
      last_value("logflare.system.observer.memory.processes", unit: {:byte, :kilobyte}),
      last_value("logflare.system.observer.memory.processes_used", unit: {:byte, :kilobyte}),
      last_value("logflare.system.observer.memory.system", unit: {:byte, :kilobyte}),
      last_value("logflare.system.observer.memory.atom", unit: {:byte, :kilobyte}),
      last_value("logflare.system.observer.memory.atom_used", unit: {:byte, :kilobyte}),
      last_value("logflare.system.observer.memory.binary", unit: {:byte, :kilobyte}),
      last_value("logflare.system.observer.memory.code", unit: {:byte, :kilobyte}),
      last_value("logflare.system.observer.memory.ets", unit: {:byte, :kilobyte}),
      last_value("logflare.system.observer.memory.persistent_term", unit: {:byte, :kilobyte}),
      last_value("logflare.system.scheduler.utilization", tags: [:name, :type])
    ]

    broadway_metrics = [
      distribution("broadway.batcher.stop.duration", unit: {:native, :millisecond}),
      distribution("broadway.batch_processor.stop.duration", unit: {:native, :millisecond}),
      distribution("broadway.processor.message.stop.duration", unit: {:native, :millisecond}),
      distribution("broadway.processor.stop.duration", unit: {:native, :millisecond})
    ]

    application_metrics = [
      distribution("logflare.goth.fetch.stop.duration",
        unit: {:native, :millisecond}
      ),
      distribution("logflare.logs.processor.ingest.stop.duration",
        tags: [:processor],
        unit: {:native, :millisecond}
      ),
      counter("logflare.total_http_requests",
        measurement: :duration,
        event_name: "bandit.request.stop.duration"
      ),
      sum("logflare.logs.processor.ingest.logs.count",
        tags: [:processor],
        description: "Total raw events ingested by processor"
      ),
      distribution("logflare.logs.processor.ingest.logs.count",
        tags: [:processor],
        description: "Distribution of log request batch sizes ingested by processor"
      ),
      distribution("logflare.backends.pipeline.handle_batch.batch_size",
        tags: [:backend_type],
        reporter_opts: batch_size_reporter_opts(),
        description: "Distribution of batch sizes for broadway pipeline by backend type"
      ),
      sum("logflare.backends.pipeline.handle_batch.batch_size",
        tags: [:backend_type],
        description: "Sum of batch sizes for broadway pipeline by backend type"
      ),
      counter("logflare.cache_buster.to_bust.count", tags: []),
      counter("logflare.logs.ingest_logs.drop",
        description: "Ingest drops"
      ),
      counter("logflare.logs.ingest_logs.rejected",
        description: "Ingest rejects"
      ),
      counter("logflare.logs.ingest_logs.buffer_full",
        description: "Ingest buffer fulls"
      ),
      counter("logflare.rate_limiter.rejected",
        description: "Rate limited API hits"
      ),
      last_value("logflare.system.finch.in_flight_requests", tags: [:pool, :url]),
      distribution("logflare.backends.dynamic_pipeline.pipeline_count"),
      distribution("logflare.ingest.pipeline.stream_batch.stop.duration",
        unit: {:native, :millisecond}
      ),
      distribution("logflare.endpoints.run_query.exec_query_on_backend.stop.duration",
        unit: {:native, :millisecond},
        description: "Endpoint query execution duration"
      ),
      last_value("logflare.system.top_processes.message_queue.length",
        tags: [:name],
        description: "Top processes by message queue length"
      ),
      last_value("logflare.system.top_processes.memory.size",
        tags: [:name],
        description: "Top processes by memory usage",
        unit: {:byte, :megabyte}
      ),
      last_value("logflare.system.top_ets_tables.individual.memory",
        tags: [:name],
        description: "Top ETS individual tables by memory usage"
      ),
      sum("logflare.system.top_ets_tables.grouped.memory",
        tags: [:name],
        description: "Top ETS tables by memory usage, grouped by name"
      ),
      sum("logflare.backends.ingest.dispatch.count",
        tags: [:backend_type],
        description: "Ingest counts by backend type"
      ),
      distribution("logflare.backends.ingest.dispatch.stop.duration",
        tags: [:backend_type],
        unit: {:native, :millisecond},
        description: "Ingest dispatch latency by backend type"
      ),
      counter("thousand_island.acceptor.spawn_error",
        description: "Count of client connection spawn errors"
      )
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

  defp periodic_measurements do
    cache_stats? = Application.get_env(:logflare, :cache_stats, false)

    cachex_metrics =
      if cache_stats? do
        [{__MODULE__, :cachex_metrics, []}]
      else
        []
      end

    process_metrics =
      [
        {__MODULE__, :process_message_queue_metrics, []},
        {__MODULE__, :process_memory_metrics, []},
        {__MODULE__, :ets_table_metrics, []}
      ]

    cachex_metrics ++ process_metrics
  end

  def cachex_metrics do
    Enum.each(@caches, fn {cache, metric} ->
      {:ok, stats} = Cachex.stats(cache)

      {:total_heap_size, total_heap_size} =
        cache
        |> Process.whereis()
        |> Process.info(:total_heap_size)

      metrics = %{
        purge: Map.get(stats, :purge, 0),
        stats: Map.get(stats, :stats, 0),
        evictions: Map.get(stats, :evictions, 0),
        expirations: Map.get(stats, :expirations, 0),
        operations: Map.get(stats, :operations, 0),
        hits: Map.get(stats, :hits, 0),
        misses: Map.get(stats, :misses, 0),
        hit_rate: Map.get(stats, :hit_rate, 0),
        miss_rate: Map.get(stats, :miss_rate, 0),
        total_heap_size: total_heap_size
      }

      :telemetry.execute([:cachex, metric], metrics)
    end)
  end

  def process_message_queue_metrics,
    do: process_attribute_metrics(:message_queue)

  def process_memory_metrics,
    do: process_attribute_metrics(:memory)

  defp process_attribute_metrics(type) do
    metric_params = @process_metrics[type]

    :recon.proc_count(metric_params.process_attribute, 10)
    |> Enum.each(fn {pid, val, call_info} ->
      [current_function, initial_call] = get_current_and_initial_call(call_info)
      name = get_display_flag(call_info, initial_call, pid)

      metrics = %{metric_params.measurement => val}

      metadata = %{
        pid: inspect(pid),
        name: name,
        current_fuction: mfa_to_string(current_function),
        initial_call: mfa_to_string(initial_call)
      }

      :telemetry.execute([:logflare, :system, :top_processes, type], metrics, metadata)
    end)
  end

  defp get_current_and_initial_call(call_info) do
    [:current_function, :initial_call]
    |> Enum.map(fn key ->
      call_info
      |> List.keyfind(key, 0)
      |> elem(1)
    end)
  end

  defp get_display_flag([possible_name | _], initial_call, pid),
    do: choose_name(possible_name) || choose_label(pid) || choose_initial_call(initial_call, pid)

  defp choose_name(name) when is_atom(name), do: name
  defp choose_name(_), do: false

  defp choose_label(pid) do
    with true <- function_exported?(:proc_lib, :get_label, 1),
         label when label != :undefined <- :proc_lib.get_label(pid) do
      :io_lib.format("~p", [label])
      |> to_string()
    else
      _ -> false
    end
  end

  defp choose_initial_call({:proc_lib, :init_p, 5}, pid) do
    pid
    |> :proc_lib.translate_initial_call()
    |> mfa_to_string()
  end

  defp choose_initial_call(call, _pid), do: mfa_to_string(call)

  defp mfa_to_string({m, f, a}), do: "#{inspect(m)}.#{f}/#{a}"

  def ets_table_metrics do
    top_100_tables = get_top_100_ets_tables_info()

    # send top 10
    top_100_tables
    |> Enum.take(10)
    |> Enum.each(fn table ->
      metrics = %{memory: table[:memory]}
      metadata = %{name: table[:name]}

      :telemetry.execute([:logflare, :system, :top_ets_tables, :individual], metrics, metadata)
    end)

    # send grouped top 100
    top_100_tables
    |> Enum.each(fn table ->
      metrics = %{memory: table[:memory]}
      metadata = %{name: ets_table_base_name(table[:name])}

      :telemetry.execute([:logflare, :system, :top_ets_tables, :grouped], metrics, metadata)
    end)
  end

  defp get_top_100_ets_tables_info do
    :ets.all()
    |> Stream.map(fn table ->
      case ets_info(table) do
        :undefined -> nil
        info -> {0, info[:memory], info}
      end
    end)
    |> Enum.filter(& &1)
    |> sort_and_take_top_100()
  end

  defp sort_and_take_top_100(items) do
    items
    |> :recon_lib.sublist_top_n_attrs(100)
    |> Enum.map(&elem(&1, 2))
  end

  @number_suffix_regex ~r/(?=.*)(\d+)$/
  defp ets_table_base_name(name) do
    name
    |> inspect()
    |> String.replace(@number_suffix_regex, "")
  end

  defp batch_size_reporter_opts do
    [buckets: [0, 1, 5, 10, 50, 100, 150, 250, 500, 1000, 2000]]
  end
end
