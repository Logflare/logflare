defmodule Logflare.Sources.Source.BigQuery.SchemaMetricsTest do
  @moduledoc false
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

  test "reports sampler deltas without per-sample telemetry" do
    SchemaMetrics.record_sample(:zero_rate)
    SchemaMetrics.record_sample(:floor)
    SchemaMetrics.record_sample(:normal)
    SchemaMetrics.record_sample(:bootstrap)
    SchemaMetrics.record_admission(:admitted)
    SchemaMetrics.record_admission(:rejected)
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
                      samples_handled: 0
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
           max_pending_samples: 40,
           plan: %{limit_source_fields_limit: 500},
           bigquery_project_id: "some-id",
           bigquery_dataset_id: "some-id",
           name: name
         ]}
      )

    :ok = :sys.suspend(pid)
    on_exit(fn -> if Process.alive?(pid), do: :sys.resume(pid) end)

    for _ <- 1..40 do
      Schema.update(name, build(:log_event, source: source), source)
    end

    Logflare.Telemetry.process_message_queue_metrics()

    assert_receive {:schema_telemetry, [:logflare, :bigquery, :schema, :queues],
                    %{
                      observed_process_count: process_count,
                      queue_length_max: queue_length_max,
                      queue_length_sum: queue_length_sum,
                      queues_above_32: queues_above_32
                    }}

    assert process_count >= 1
    assert queue_length_max >= 40
    assert queue_length_sum >= 40
    assert queues_above_32 >= 1
  end

  test "queue threshold metrics exclude queues exactly at the threshold" do
    user = insert(:user)
    source = insert(:source, user: user, lock_schema: true)
    name = Backends.via_source(source, Schema, nil)

    pid =
      start_supervised!(
        {Schema,
         [
           source: source,
           max_pending_samples: 1_001,
           plan: %{limit_source_fields_limit: 500},
           bigquery_project_id: "some-id",
           bigquery_dataset_id: "some-id",
           name: name
         ]}
      )

    :ok = :sys.suspend(pid)
    on_exit(fn -> if Process.alive?(pid), do: :sys.resume(pid) end)
    event = build(:log_event, source: source)

    for _ <- 1..32, do: Schema.update(name, event, source)
    assert_queue_thresholds(pid, 32, {0, 0, 0})

    Schema.update(name, event, source)
    assert_queue_thresholds(pid, 33, {1, 0, 0})

    for _ <- 34..100, do: Schema.update(name, event, source)
    assert_queue_thresholds(pid, 100, {1, 0, 0})

    Schema.update(name, event, source)
    assert_queue_thresholds(pid, 101, {1, 1, 0})

    for _ <- 102..1_000, do: Schema.update(name, event, source)
    assert_queue_thresholds(pid, 1_000, {1, 1, 0})

    Schema.update(name, event, source)
    assert_queue_thresholds(pid, 1_001, {1, 1, 1})
  end

  defp assert_queue_thresholds(pid, length, {above_32, above_100, above_1000}) do
    assert {:message_queue_len, ^length} = Process.info(pid, :message_queue_len)
    Logflare.Telemetry.process_message_queue_metrics()

    assert_receive {:schema_telemetry, [:logflare, :bigquery, :schema, :queues], measurements}
    assert measurements.observed_process_count >= 1
    assert measurements.queues_above_32 == above_32
    assert measurements.queues_above_100 == above_100
    assert measurements.queues_above_1000 == above_1000
  end
end
