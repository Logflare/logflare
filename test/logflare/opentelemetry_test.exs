defmodule Logflare.OpenTelemetryTest do
  use Logflare.DataCase, async: false

  require OpenTelemetry.Tracer
  require Record

  use ExUnitProperties

  alias Broadway.Message
  alias Logflare.Sources.Source.BigQuery.Pipeline

  @span_fields Record.extract(:span, from: "deps/opentelemetry/include/otel_span.hrl")
  Record.defrecordp(:span, @span_fields)

  setup do
    :ok = :otel_simple_processor.set_exporter(:otel_exporter_pid, self())
    on_exit(fn -> :otel_simple_processor.set_exporter(:none) end)

    insert(:plan)
    user = insert(:user)
    source = insert(:source, user_id: user.id)
    [source: source]
  end

  describe "only ingest spans are emitted in" do
    test "handle_batch (streaming insert)", %{source: source} do
      stub(Logflare.Google.BigQuery, :stream_batch!, fn _context, _rows ->
        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      le = build(:log_event, source: source)
      messages = [%Message{data: le, acknowledger: {Pipeline, :ack_id, :ack_data}}]
      batch_info = %Broadway.BatchInfo{batcher: :bq, batch_key: :bq, size: 1, trigger: :flush}

      context = %{
        source_id: source.id,
        source_token: source.token,
        backend_id: nil,
        bigquery_project_id: nil,
        bigquery_dataset_id: nil,
        user_id: source.user_id,
        system_source: source.system_source
      }

      Pipeline.handle_batch(:bq, messages, batch_info, context)

      spans = collect_spans()
      assert length(spans) > 0

      for span <- spans do
        assert span(name: "ingest." <> _) = span
      end
    end

    test "handle_batch (storage write api)", %{source: source} do
      source = insert(:source, user_id: source.user_id, bq_storage_write_api: true)

      stub(
        Logflare.Backends.Adaptor.BigQueryAdaptor.GoogleApiClient,
        :append_rows,
        fn _rows, _context, _table -> :ok end
      )

      le = build(:log_event, source: source)
      messages = [%Message{data: le, acknowledger: {Pipeline, :ack_id, :ack_data}}]
      batch_info = %Broadway.BatchInfo{batcher: :bq, batch_key: :bq, size: 1, trigger: :flush}

      context = %{
        source_id: source.id,
        source_token: source.token,
        backend_id: nil,
        bigquery_project_id: nil,
        bigquery_dataset_id: nil,
        user_id: source.user_id,
        system_source: source.system_source
      }

      Pipeline.handle_batch(:bq, messages, batch_info, context)

      spans = collect_spans()
      assert length(spans) > 0

      for span <- spans do
        assert span(name: "ingest." <> _) = span
      end
    end
  end

  defp collect_spans(acc \\ []) do
    receive do
      {:span, s} -> collect_spans([s | acc])
    after
      0 -> acc
    end
  end
end
