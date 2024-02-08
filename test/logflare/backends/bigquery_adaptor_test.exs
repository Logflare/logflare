defmodule Logflare.Backends.BigQueryAdaptorTest do
  use Logflare.DataCase, async: false
  use Mimic

  alias Logflare.Backends.Adaptor
  alias Logflare.Source.RecentLogsServer, as: RLS

  @subject Logflare.Backends.Adaptor.BigQueryAdaptor

  doctest @subject

  setup do
    config = %{}

    source = insert(:source, user: insert(:user))

    source_backend =
      insert(:source_backend,
        type: :bigquery,
        source: source,
        config: config
      )

    _rls = start_supervised %{
      start: {RLS, :start_link, %RLS{source_id: source.token}}
    }

    adaptor = start_supervised! Adaptor.child_spec(source_backend)

    {:ok, source: source, source_backend: source_backend, adaptor: adaptor}
  end

  test "plain ingest", %{adaptor: adaptor, source_backend: source_backend} do
    log_event = build(:log_event, source: source_backend.source, test: "data")

    Logflare.Google.BigQuery
    |> expect(:stream_batch!, fn _, _ ->
      {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
    end)

    assert [%{data: log_event}] == @subject.ingest(adaptor, [%{data: log_event}])
  end

  test "update table", %{adaptor: adaptor, source_backend: source_backend} do
    log_event = build(:log_event, lock_schema: false, source: source_backend.source, test: "data")

    Logflare.Google.BigQuery
    |> stub(:stream_batch!, fn _, _ ->
      {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
    end)

    Logflare.Source.BigQuery.Schema
    |> expect(:update, fn _, _ -> :ok end)

    assert [%{data: log_event}] == @subject.ingest(adaptor, [%{data: log_event}])
  end
end
