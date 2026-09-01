defmodule Logflare.TelemetryTest do
  use Logflare.DataCase, async: false

  alias Logflare.SystemMetrics.Observer
  alias Logflare.SystemMetrics.Schedulers
  alias Logflare.Telemetry
  alias Logflare.TestUtils
  alias OtelMetricExporter.MetricStore

  @clickhouse_batch_exporter :logflare_clickhouse_batch_metrics_test
  @clickhouse_batch_metric_name [
    :logflare,
    :backends,
    :clickhouse,
    :pipeline,
    :handle_batch,
    :batch_size
  ]
  @clickhouse_batch_metric_string "logflare.backends.clickhouse.pipeline.handle_batch.batch_size"
  @broadway_processor_message_event [:broadway, :processor, :message, :stop]
  @broadway_processor_message_exporter :logflare_broadway_processor_message_metrics_test
  @broadway_processor_message_metric_name [:broadway, :processor, :message, :stop, :duration]
  @broadway_processor_message_metric_string "broadway.processor.message.stop.duration"

  @drop_stale_exporter :logflare_drop_stale_metrics_test
  @drop_stale_metric_name [:logflare, :logs, :ingest_logs, :drop_stale]
  @drop_stale_metric_string "logflare.logs.ingest_logs.drop_stale"

  @requeue_deduplicated_metric_name [
    :logflare,
    :ingest_event_queue,
    :requeue_deduplicated,
    :count
  ]

  describe "metrics/0" do
    test "returns only well-formed Telemetry.Metrics definitions" do
      metrics = Telemetry.metrics()

      assert is_list(metrics)
      assert metrics != []

      # Checked structurally (every Telemetry.Metrics.* struct has :name and
      # :event_name) rather than against a hardcoded list of struct modules —
      # this file's `alias Logflare.Telemetry` above shadows the bare
      # `Telemetry` name, so a literal `Telemetry.Metrics.Counter` here would
      # silently resolve to the nonexistent `Logflare.Telemetry.Metrics.Counter`
      # instead of the real dependency's struct.
      assert Enum.all?(metrics, fn metric ->
               is_struct(metric) and
                 to_string(metric.__struct__) =~ "Telemetry.Metrics." and
                 is_list(metric.name) and
                 is_list(metric.event_name)
             end)
    end

    test "includes the spool telemetry metrics added for throttling/storage/queue observability" do
      names = Telemetry.metrics() |> Enum.map(& &1.name)

      for expected <- [
            [:logflare, :backends, :spool, :throttled, :throttled],
            [:logflare, :backends, :spool, :storage, :put, :count],
            [:logflare, :backends, :spool, :storage, :get, :count],
            [:logflare, :backends, :spool, :queue, :publish, :count],
            [:logflare, :backends, :spool, :queue, :receive, :count],
            [:logflare, :backends, :spool, :queue, :ack, :count],
            [:logflare, :backends, :spool, :queue, :nack, :count],
            [:logflare, :backends, :spool, :producer, :batch, :count]
          ] do
        assert expected in names, "expected #{inspect(expected)} to be a defined metric"
      end
    end

    test "defines retry deduplication as a low-cardinality event count" do
      [metric] = requeue_deduplicated_metrics()

      assert metric.event_name == [:logflare, :ingest_event_queue, :requeue_deduplicated]
      assert metric.measurement == :count
      assert metric.tags == [:backend_type]
    end

    test "defines ClickHouse batch distribution and throughput metrics" do
      metrics = clickhouse_batch_metrics()

      assert length(metrics) == 2

      assert Enum.sort(Enum.map(metrics, &to_string(&1.__struct__))) == [
               "Elixir.Telemetry.Metrics.Distribution",
               "Elixir.Telemetry.Metrics.Sum"
             ]

      for metric <- metrics do
        assert metric.event_name == [:logflare, :backends, :pipeline, :handle_batch]
        assert metric.measurement == :batch_size
        assert metric.tags == [:backend_id, :event_type, :batch_trigger]
        assert metric.keep.(%{backend_type: :clickhouse})
        refute metric.keep.(%{backend_type: :bigquery})
      end
    end

    test "honors configured Broadway processor message duration sampling" do
      denominator = Application.fetch_env!(:logflare, :broadway_message_sample_denominator)

      metric =
        Enum.find(Telemetry.metrics(), fn metric ->
          metric.name == @broadway_processor_message_metric_name and
            metric.event_name == @broadway_processor_message_event
        end)

      if denominator == :disabled do
        assert metric == nil
      else
        assert is_function(metric.measurement, 1)
        assert metric.unit == :millisecond

        contexts = Enum.map(1..10_000, fn _ -> make_ref() end)

        accepted_contexts =
          case denominator do
            1 ->
              assert metric.keep == nil
              contexts

            denominator ->
              assert is_function(metric.keep, 1)
              refute metric.keep.(%{})

              Enum.filter(contexts, fn context ->
                metadata = %{telemetry_span_context: context}
                expected = :erlang.phash2(context, denominator) == 0

                assert metric.keep.(metadata) == expected
                assert metric.keep.(metadata) == expected

                expected
              end)
          end

        start_supervised!(
          {OtelMetricExporter,
           name: @broadway_processor_message_exporter,
           metrics: [metric],
           export_period: :timer.minutes(5),
           otlp_protocol: :http_protobuf,
           otlp_endpoint: "http://localhost:4318",
           otlp_headers: %{},
           otlp_compression: nil}
        )

        :telemetry.execute(
          @broadway_processor_message_event,
          %{duration: System.convert_time_unit(1, :millisecond, :native)},
          %{}
        )

        Enum.each(contexts, fn context ->
          :telemetry.execute(
            @broadway_processor_message_event,
            %{duration: System.convert_time_unit(1, :millisecond, :native)},
            %{telemetry_span_context: context}
          )
        end)

        observed_count =
          case MetricStore.get_metrics(@broadway_processor_message_exporter)[
                 {:distribution, @broadway_processor_message_metric_string}
               ] do
            nil ->
              0

            distributions ->
              distributions
              |> Map.fetch!(%{})
              |> Map.values()
              |> Enum.sum_by(fn {count, _sum} -> count end)
          end

        malformed_metadata_count = if denominator == 1, do: 1, else: 0
        assert observed_count == length(accepted_contexts) + malformed_metadata_count
      end
    end

    test "aggregates ClickHouse batches by backend, event type, and trigger" do
      start_supervised!(
        {OtelMetricExporter,
         name: @clickhouse_batch_exporter,
         metrics: clickhouse_batch_metrics(),
         export_period: :timer.minutes(5),
         otlp_protocol: :http_protobuf,
         otlp_endpoint: "http://localhost:4318",
         otlp_headers: %{},
         otlp_compression: nil}
      )

      event = [:logflare, :backends, :pipeline, :handle_batch]
      log_tags = %{backend_id: 1, event_type: :log, batch_trigger: :size}
      other_log_tags = %{backend_id: 2, event_type: :log, batch_trigger: :size}
      metric_tags = %{backend_id: 1, event_type: :metric, batch_trigger: :timeout}
      trace_tags = %{backend_id: 2, event_type: :trace, batch_trigger: :timeout}

      :telemetry.execute(
        event,
        %{batch_size: 20_000},
        Map.put(log_tags, :backend_type, :clickhouse)
      )

      :telemetry.execute(
        event,
        %{batch_size: 10_000},
        Map.put(other_log_tags, :backend_type, :clickhouse)
      )

      :telemetry.execute(
        event,
        %{batch_size: 500},
        Map.put(metric_tags, :backend_type, :clickhouse)
      )

      :telemetry.execute(
        event,
        %{batch_size: 125},
        Map.put(trace_tags, :backend_type, :clickhouse)
      )

      :telemetry.execute(event, %{batch_size: 999}, %{backend_type: :bigquery})

      assert %{
               {:distribution, @clickhouse_batch_metric_string} => distributions,
               {:sum, @clickhouse_batch_metric_string} => sums
             } = MetricStore.get_metrics(@clickhouse_batch_exporter)

      assert sums == %{
               log_tags => 20_000,
               other_log_tags => 10_000,
               metric_tags => 500,
               trace_tags => 125
             }

      assert [{_bucket, {1, 20_000}}] = distributions |> Map.fetch!(log_tags) |> Map.to_list()

      assert [{_bucket, {1, 10_000}}] =
               distributions |> Map.fetch!(other_log_tags) |> Map.to_list()

      assert [{_bucket, {1, 500}}] = distributions |> Map.fetch!(metric_tags) |> Map.to_list()
      assert [{_bucket, {1, 125}}] = distributions |> Map.fetch!(trace_tags) |> Map.to_list()
    end

    test "tags stale event drops by backend" do
      [metric] = drop_stale_metrics()

      assert metric.event_name == @drop_stale_metric_name
      assert metric.measurement == :count
      assert metric.tags == [:backend_id, :backend_type]
    end

    test "keeps only stale event drops carrying backend metadata" do
      [metric] = drop_stale_metrics()

      assert metric.keep.(%{backend_id: 1, backend_type: :clickhouse})
      refute metric.keep.(%{source_id: 1, source_token: "abc"})
      refute metric.keep.(%{backend_id: 1})
      refute metric.keep.(%{backend_type: :clickhouse})
    end

    test "aggregates stale event drops per backend without source-level cardinality" do
      start_supervised!(
        {OtelMetricExporter,
         name: @drop_stale_exporter,
         metrics: drop_stale_metrics(),
         export_period: :timer.minutes(5),
         otlp_protocol: :http_protobuf,
         otlp_endpoint: "http://localhost:4318",
         otlp_headers: %{},
         otlp_compression: nil}
      )

      backend_one = %{backend_id: 1, backend_type: :clickhouse}
      backend_two = %{backend_id: 2, backend_type: :clickhouse}

      for {backend_tags, source_id, count} <- [
            {backend_one, 100, 5},
            {backend_one, 200, 7},
            {backend_two, 100, 3}
          ] do
        metadata =
          backend_tags
          |> Map.put(:source_id, source_id)
          |> Map.put(:source_token, "source-#{source_id}")

        :telemetry.execute(@drop_stale_metric_name, %{count: count}, metadata)
      end

      :telemetry.execute(@drop_stale_metric_name, %{count: 99}, %{source_id: 300})

      assert %{{:sum, @drop_stale_metric_string} => sums} =
               MetricStore.get_metrics(@drop_stale_exporter)

      assert sums == %{backend_one => 12, backend_two => 3}
    end
  end

  describe "service_attributes/1 commit normalization" do
    test "trims surrounding whitespace from the commit" do
      assert %{commit: "abc123"} = Telemetry.service_attributes("  abc123\n")
    end

    test "omits commit when the SHA is empty or whitespace-only" do
      refute Map.has_key?(Telemetry.service_attributes(""), :commit)
      refute Map.has_key?(Telemetry.service_attributes("   "), :commit)
    end
  end

  describe "process metrics" do
    test "retrieves and emits top 10 by memory" do
      event = [:logflare, :system, :top_processes, :memory]
      TestUtils.attach_forwarder(event)
      Telemetry.process_memory_metrics()

      assert_receive {:telemetry_event, ^event, metrics, meta}
      assert match?(%{size: _}, metrics)
      assert match?(%{name: _}, meta)
    end

    test "retrieves and emits top 10 by message queue" do
      event = [:logflare, :system, :top_processes, :message_queue]
      TestUtils.attach_forwarder(event)
      Telemetry.process_message_queue_metrics()

      assert_receive {:telemetry_event, ^event, metrics, meta}
      assert match?(%{length: _}, metrics)
      assert match?(%{name: _}, meta)
    end
  end

  describe "ets_table_metrics/1" do
    test "retrieves and emits top 10 by memory usage" do
      event = [:logflare, :system, :top_ets_tables, :individual]
      TestUtils.attach_forwarder(event)
      Telemetry.ets_table_metrics()

      assert_receive {:telemetry_event, ^event, metrics, meta}
      assert match?(%{memory: _}, metrics)
      assert match?(%{name: _}, meta)
    end

    test "retrieves and emits top 100 by memory usage" do
      event = [:logflare, :system, :top_ets_tables, :grouped]
      TestUtils.attach_forwarder(event)
      Telemetry.ets_table_metrics()

      assert_receive {:telemetry_event, ^event, metrics, meta}
      assert match?(%{memory: _}, metrics)
      assert match?(%{name: _}, meta)
    end

    test "ignores tables that were deleted during listing" do
      # simulates all tables being deleted to simplify testing, so we can
      # just check if all tables were skipped for returning :undefined
      Logflare.Utils
      |> stub(:ets_info, fn _ -> :undefined end)

      event = [:logflare, :system, :top_ets_tables, :individual]
      TestUtils.attach_forwarder(event)
      Telemetry.ets_table_metrics()

      refute_receive {:telemetry_event, ^event, _, _}
    end
  end

  test "observer metrics" do
    ref =
      :telemetry_test.attach_event_handlers(self(), [
        [:logflare, :system, :observer, :metrics],
        [:logflare, :system, :observer, :memory]
      ])

    on_exit(fn -> :telemetry.detach(ref) end)
    Observer.dispatch_stats()

    assert_receive {[:logflare, :system, :observer, :metrics], ^ref, metrics_measurements,
                    metrics_metadata}

    assert_receive {[:logflare, :system, :observer, :memory], ^ref, memory_measurements,
                    memory_metadata}

    assert metrics_measurements |> Map.keys() |> Enum.sort() == [
             :atom_count,
             :atom_limit,
             :ets_count,
             :ets_limit,
             :io_input,
             :io_output,
             :logical_processors,
             :logical_processors_online,
             :otp_release,
             :port_count,
             :port_limit,
             :process_count,
             :process_limit,
             :run_queue,
             :schedulers_online,
             :total_active_tasks,
             :uptime
           ]

    # All measurement values must be numeric (integers or floats)
    for {key, value} <- metrics_measurements do
      assert is_number(value), "expected #{key} to be numeric, got: #{inspect(value)}"
    end

    assert metrics_metadata == %{}

    assert memory_measurements |> Map.keys() |> Enum.sort() == [
             :atom,
             :atom_used,
             :binary,
             :code,
             :ets,
             :persistent_term,
             :processes,
             :processes_used,
             :system,
             :total
           ]

    for {key, value} <- memory_measurements do
      assert is_number(value), "expected #{key} to be numeric, got: #{inspect(value)}"
    end

    assert memory_metadata == %{}
  end

  test "scheduler metrics" do
    event = [:logflare, :system, :scheduler]
    ref = :telemetry_test.attach_event_handlers(self(), [event])
    on_exit(fn -> :telemetry.detach(ref) end)

    sample_duration = to_timeout(millisecond: 10)
    Schedulers.collect_dispatch_stats(sample_duration)

    assert_receive {^event, ^ref, %{utilization: _}, %{name: "total", type: "total"}}
    assert_receive {^event, ^ref, %{utilization: _}, %{name: "weighted", type: "weighted"}}

    for id <- 1..:erlang.system_info(:schedulers) do
      id = Integer.to_string(id)
      assert_receive {^event, ^ref, %{utilization: _}, %{name: ^id, type: "normal"}}
    end

    dirty_cpu_offset = :erlang.system_info(:schedulers)

    for id <-
          (dirty_cpu_offset + 1)..(dirty_cpu_offset + :erlang.system_info(:dirty_cpu_schedulers)) do
      id = Integer.to_string(id)
      assert_receive {^event, ^ref, %{utilization: _}, %{name: ^id, type: "dirty"}}
    end

    dirty_io_offset = dirty_cpu_offset + :erlang.system_info(:dirty_cpu_schedulers)

    for id <- (dirty_io_offset + 1)..(dirty_io_offset + :erlang.system_info(:dirty_io_schedulers)) do
      id = Integer.to_string(id)
      assert_receive {^event, ^ref, %{utilization: _}, %{name: ^id, type: "dirty (io)"}}
    end
  end

  describe "cachex_metrics/0" do
    test "retrieves and emits stats for caches with all metrics" do
      expected_metrics = [
        :log_events,
        :rejected_log_events,
        :pub_sub_rates,
        :team_users,
        :partners,
        :users,
        :backends,
        :sources,
        :billing,
        :source_schemas,
        :auth,
        :endpoints,
        :rules,
        :key_values,
        :saved_searches
      ]

      events = Enum.map(expected_metrics, fn metric -> [:cachex, metric] end)
      ref = :telemetry_test.attach_event_handlers(self(), events)
      on_exit(fn -> :telemetry.detach(ref) end)

      # simulates telemetry_poller tick
      Telemetry.cachex_metrics()

      for event <- events do
        assert_receive {^event, ^ref, measurements, metadata}

        assert measurements |> Map.keys() |> Enum.sort() == [
                 :evictions,
                 :expirations,
                 :hit_rate,
                 :hits,
                 :miss_rate,
                 :misses,
                 :operations,
                 :purge,
                 :stats,
                 :total_heap_size
               ]

        for {key, value} <- measurements do
          assert is_number(value),
                 "expected #{key} for #{inspect(event)} to be numeric, got: #{inspect(value)}"
        end

        assert metadata == %{}
      end

      refute_received _anything_else
    end
  end

  defp clickhouse_batch_metrics do
    Enum.filter(Telemetry.metrics(), &(&1.name == @clickhouse_batch_metric_name))
  end

  defp drop_stale_metrics do
    Enum.filter(Telemetry.metrics(), &(&1.name == @drop_stale_metric_name))
  end

  defp requeue_deduplicated_metrics do
    Enum.filter(Telemetry.metrics(), &(&1.name == @requeue_deduplicated_metric_name))
  end
end
