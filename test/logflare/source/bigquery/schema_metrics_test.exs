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
    SchemaMetrics.record_admission(:admitted)
    SchemaMetrics.record_admission(:rejected)
    SchemaMetrics.record_handled()

    assert :ok = SchemaMetrics.report()

    assert_receive {:schema_telemetry, [:logflare, :bigquery, :schema, :report],
                    %{
                      samples_selected: 3,
                      samples_selected_zero_rate: 1,
                      samples_selected_floor: 1,
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
end
