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
            instance: inspect(Node.self())
          })
          |> Keyword.update!(:otlp_headers, &Map.new/1)

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
      last_value("logflare.system.observer.metrics.total_active_tasks")
    ]

    broadway_metrics = [
      distribution("broadway.batcher.stop.duration", unit: {:native, :millisecond}),
      distribution("broadway.batch_processor.stop.duration", unit: {:native, :millisecond}),
      distribution("broadway.processor.message.stop.duration", unit: {:native, :millisecond}),
      distribution("broadway.processor.stop.duration", unit: {:native, :millisecond})
    ]

    application_metrics = [
      distribution("logflare.goth.fetch.stop.duration",
        tags: [:partition],
        unit: {:native, :millisecond}
      ),
      distribution("logflare.logs.processor.ingest.stop.duration",
        tags: [:processor],
        unit: {:native, :millisecond}
      ),
      counter("logflare.logs.processor.ingest.stop.duration",
        tags: [:processor],
        description: "Ingestion execution counts"
      ),
      distribution("logflare.logs.processor.ingest.logs.count",
        tags: [:processor],
        description: "Ingestion batch size"
      ),
      distribution("logflare.logs.processor.ingest.store.stop.duration",
        tags: [:processor],
        unit: {:native, :millisecond}
      ),
      distribution("logflare.logs.processor.ingest.handle_batch.stop.duration",
        tags: [:processor],
        unit: {:native, :millisecond}
      ),
      distribution("logflare.ingest.pipeline.handle_batch.batch_size",
        tags: [:pipeline],
        reporter_opts: batch_size_reporter_opts()
      ),
      distribution("logflare.ingest.common_pipeline.handle_batch.batch_size",
        tags: [:pipeline],
        reporter_opts: batch_size_reporter_opts()
      ),
      counter("logflare.context_cache.busted.count", tags: [:schema, :table]),
      counter("logflare.context_cache.handle_record.count", tags: [:schema, :table]),
      counter("logflare.logs.ingest_logs.drop",
        tags: [],
        description: "Ingest drops"
      ),
      counter("logflare.logs.ingest_logs.rejected",
        tags: [],
        description: "Ingest rejects"
      ),
      counter("logflare.logs.ingest_logs.buffer_full",
        tags: [],
        description: "Ingest buffer fulls"
      ),
      counter("logflare.rate_limiter.rejected",
        tags: [],
        description: "Rate limited API hits"
      ),
      last_value("logflare.backends.egress.request_length", tags: [:backend_id]),
      last_value("logflare.system.finch.in_flight_requests", tags: [:pool, :url]),
      last_value("logflare.google.set_iam_policy.members",
        description: "Google IAM policy members count"
      ),
      last_value("logflare.backends.dynamic_pipeline.pipeline_count",
        tags: [:backend_id]
      ),
      last_value("logflare.backends.ingest_event_queue.queue_janitor.length",
        tags: [:backend_id]
      ),
      distribution("logflare.ingest.pipeline.stream_batch.stop.duration",
        tags: [:source_token],
        unit: {:native, :millisecond}
      ),
      last_value("logflare.backends.dynamic_pipeline.increment.success_count",
        tags: [:backend_id],
        description: "Dynamic pipeline sucessfully increment count"
      ),
      last_value("logflare.backends.dynamic_pipeline.increment.error_count",
        tags: [:backend_id],
        description: "Dynamic pipeline failed increment count"
      ),
      last_value("logflare.backends.dynamic_pipeline.decrement.success_count",
        tags: [:backend_id],
        description: "Dynamic pipeline sucessfully decrement count"
      ),
      last_value("logflare.backends.dynamic_pipeline.decrement.error_count",
        tags: [:backend_id],
        description: "Dynamic pipeline failed decrement count"
      ),
      distribution("logflare.endpoints.run_query.exec_query_on_backend.stop.duration",
        tags: [:endpoint_id],
        unit: {:native, :millisecond},
        description: "Endpoint query execution duration"
      ),
      counter("logflare.endpoints.run_query.exec_query_on_backend.stop.duration",
        tags: [:endpoint_id],
        description: "Endpoint query execution counts"
      ),
      distribution("logflare.endpoints.run_query.exec_query_on_backend.total_rows",
        tags: [:endpoint_id],
        description: "Number of rows returned by endpoint query execution"
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
      last_value("logflare.system.top_ets_tables.individual.size",
        tags: [:name],
        description: "Top ETS individual tables by size"
      ),
      sum("logflare.system.top_ets_tables.grouped.size",
        tags: [:name],
        description: "Top ETS tables by size, grouped by name"
      ),
      sum("logflare.backends.ingest.count",
        tags: [:backend_type],
        description: "Ingest counts by backend type"
      ),
      sum("logflare.backends.clickhouse.ingest.count",
        tags: [],
        description: "ClickHouse ingest counts by source id"
      ),
      distribution("logflare.backends.ingest.dispatch.stop.duration",
        tags: [:backend_type],
        unit: {:native, :millisecond},
        description: "Ingest dispatch latency by backend type"
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
      metrics = %{size: table[:size]}
      metadata = %{name: table[:name]}

      :telemetry.execute([:logflare, :system, :top_ets_tables, :individual], metrics, metadata)
    end)

    # send grouped top 100
    top_100_tables
    |> Enum.each(fn table ->
      metrics = %{size: table[:size]}
      metadata = %{name: ets_table_base_name(table[:name])}

      :telemetry.execute([:logflare, :system, :top_ets_tables, :grouped], metrics, metadata)
    end)
  end

  defp get_top_100_ets_tables_info do
    :ets.all()
    |> Stream.map(fn table ->
      case :ets.info(table) do
        :undefined -> nil
        info -> {0, info[:size], info}
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
