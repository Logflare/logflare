defmodule Logflare.Sources.Source.BigQuery.SchemaMetricsTest do
  use Logflare.DataCase

  alias Logflare.Backends
  alias Logflare.Sources.Source.BigQuery.Schema
  alias Logflare.Sources.Source.BigQuery.SchemaMetrics

  setup do
    SchemaMetrics.reset()

    handler_id = "schema-metrics-#{System.unique_integer()}"

    :telemetry.attach_many(
      handler_id,
      [
        [:logflare, :bigquery, :schema, :report],
        [:logflare, :bigquery, :schema, :queues]
      ],
      fn event, measurements, _metadata, pid ->
        send(pid, {:schema_telemetry, event, measurements})
      end,
      self()
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok
  end

  setup context do
    if context[:isolated_metrics] do
      {:dictionary, dictionary} = Process.info(Process.whereis(SchemaMetrics), :dictionary)
      [supervisor | _] = Keyword.fetch!(dictionary, :"$ancestors")
      :ok = Supervisor.terminate_child(supervisor, SchemaMetrics)
      on_exit(fn -> {:ok, _pid} = Supervisor.restart_child(supervisor, SchemaMetrics) end)
    end

    :ok
  end

  test "reports sampler deltas without per-sample telemetry" do
    SchemaMetrics.record_sample(:zero_rate)
    SchemaMetrics.record_sample(:floor)
    SchemaMetrics.record_sample(:normal)
    SchemaMetrics.record_sample(:bootstrap)
    SchemaMetrics.record_admission(:admitted)
    SchemaMetrics.record_admission(:rejected)
    SchemaMetrics.record_admission(:unavailable)
    SchemaMetrics.record_handled()

    assert :ok = SchemaMetrics.report()

    assert_receive {:schema_telemetry, [:logflare, :bigquery, :schema, :report],
                    %{
                      samples_selected: 4,
                      samples_selected_zero_rate: 1,
                      samples_selected_floor: 1,
                      samples_selected_bootstrap: 1,
                      samples_admitted: 1,
                      samples_rejected: 1,
                      samples_unavailable: 1,
                      samples_handled: 1
                    }}

    assert :ok = SchemaMetrics.report()

    assert_receive {:schema_telemetry, [:logflare, :bigquery, :schema, :report],
                    %{
                      samples_selected: 0,
                      samples_selected_zero_rate: 0,
                      samples_selected_floor: 0,
                      samples_selected_bootstrap: 0,
                      samples_admitted: 0,
                      samples_rejected: 0,
                      samples_unavailable: 0,
                      samples_handled: 0
                    }}
  end

  test "defines node-level admission counters and queue aggregates" do
    metrics = Logflare.Telemetry.metrics()

    unavailable =
      Enum.find(metrics, fn metric ->
        metric.name == [:logflare, :bigquery, :schema, :samples, :unavailable]
      end)

    assert %Telemetry.Metrics.Sum{} = unavailable
    assert unavailable.event_name == [:logflare, :bigquery, :schema, :report]
    assert unavailable.measurement == :samples_unavailable
    assert unavailable.tags == []

    queue_metrics =
      Enum.filter(metrics, &(&1.event_name == [:logflare, :bigquery, :schema, :queues]))

    assert Enum.sort(Enum.map(queue_metrics, & &1.measurement)) ==
             [:observed_process_count, :queue_length_max, :queue_length_sum]

    assert Enum.all?(queue_metrics, &(&1.tags == []))
  end

  @tag :isolated_metrics
  test "poller keeps reporting after the metrics worker exits during a call" do
    assert :ok = SchemaMetrics.report()

    worker =
      start_supervised!(
        {Task,
         fn ->
           receive do
             {:"$gen_call", _from, :report} -> exit(:shutdown)
           end
         end}
      )

    Process.register(worker, SchemaMetrics)
    monitor = Process.monitor(worker)
    measurement = {SchemaMetrics, :report, []}

    poller =
      start_supervised!(
        {:telemetry_poller,
         name: :schema_metrics_test_poller,
         measurements: [measurement],
         init_delay: 60_000,
         period: 60_000}
      )

    send(poller, :collect)
    assert :telemetry_poller.list_measurements(poller) == [measurement]
    assert_receive {:DOWN, ^monitor, :process, ^worker, :shutdown}

    start_supervised!(SchemaMetrics)
    SchemaMetrics.record_sample(:normal)
    send(poller, :collect)
    assert :telemetry_poller.list_measurements(poller) == [measurement]

    assert_receive {:schema_telemetry, [:logflare, :bigquery, :schema, :report],
                    %{samples_selected: 1}}
  end

  @tag :isolated_metrics
  test "supervisor shutdown clears counters and restart creates fresh counters" do
    key = {SchemaMetrics, :counters}
    start_supervised!(SchemaMetrics)
    counters = :persistent_term.get(key)
    SchemaMetrics.record_sample(:normal)

    assert :ok = stop_supervised(SchemaMetrics)
    assert :persistent_term.get(key, nil) == nil
    assert :ok = SchemaMetrics.record_sample(:normal)
    assert :ok = SchemaMetrics.report()

    start_supervised!(SchemaMetrics)
    refute :persistent_term.get(key) == counters
    SchemaMetrics.record_sample(:bootstrap)
    assert :ok = SchemaMetrics.report()

    assert_receive {:schema_telemetry, [:logflare, :bigquery, :schema, :report],
                    %{samples_selected: 1, samples_selected_bootstrap: 1}}
  end

  test "counts missing Schema registrations separately from full queues" do
    user = insert(:user)
    source = insert(:source, user: user, lock_schema: true)
    name = Backends.via_source(source, Schema, nil)
    event = build(:log_event, source: source)

    assert :ok = Schema.update(name, event, source)

    pid =
      start_supervised!(
        {Schema,
         [
           source: source,
           plan: %{limit_source_fields_limit: 500},
           bigquery_project_id: "some-id",
           bigquery_dataset_id: "some-id",
           name: name
         ]}
      )

    :ok = :sys.suspend(pid)
    on_exit(fn -> if Process.alive?(pid), do: :sys.resume(pid) end)

    for _ <- 1..9, do: assert(:ok = Schema.update(name, event, source))
    assert :ok = SchemaMetrics.report()

    assert_receive {:schema_telemetry, [:logflare, :bigquery, :schema, :report],
                    %{
                      samples_admitted: 8,
                      samples_rejected: 1,
                      samples_unavailable: 1,
                      samples_handled: 0
                    }}

    :ok = :sys.resume(pid)
    :sys.get_state(pid)
    assert :ok = SchemaMetrics.report()

    assert_receive {:schema_telemetry, [:logflare, :bigquery, :schema, :report],
                    %{
                      samples_admitted: 0,
                      samples_rejected: 0,
                      samples_unavailable: 0,
                      samples_handled: 8
                    }}
  end

  test "aggregates Schema queues without source-level metric dimensions" do
    user = insert(:user)
    source = insert(:source, user: user, lock_schema: true)

    name = Backends.via_source(source, Schema, nil)

    pid =
      start_supervised!(
        {Schema,
         [
           source: source,
           plan: %{limit_source_fields_limit: 500},
           bigquery_project_id: "some-id",
           bigquery_dataset_id: "some-id",
           name: name
         ]}
      )

    :ok = :sys.suspend(pid)
    on_exit(fn -> if Process.alive?(pid), do: :sys.resume(pid) end)

    for _ <- 1..8 do
      Schema.update(name, build(:log_event, source: source), source)
    end

    assert {:message_queue_len, 8} = Process.info(pid, :message_queue_len)
    Logflare.Telemetry.process_message_queue_metrics()

    assert_receive {:schema_telemetry, [:logflare, :bigquery, :schema, :queues],
                    %{
                      observed_process_count: process_count,
                      queue_length_max: queue_length_max,
                      queue_length_sum: queue_length_sum
                    }}

    assert process_count >= 1
    assert queue_length_max >= 8
    assert queue_length_sum >= 8
  end
end
