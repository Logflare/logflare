defmodule Logflare.Backends.BigQueryAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor
  alias Logflare.Source.RecentLogsServer, as: RLS

  @subject Logflare.Backends.Adaptor.BigQueryAdaptor

  doctest @subject

  setup do
    config = %{
      project_id: "some-project",
      dataset_id: "some-dataset"
    }

    source = insert(:source, user: insert(:user))

    backend =
      insert(:backend,
        type: :bigquery,
        sources: [source],
        config: config
      )

    _rls = start_supervised %{
      start: {RLS, :start_link, %RLS{source_id: source.token}}
    }

    adaptor = start_supervised!(Adaptor.child_spec(source, backend))

    {:ok, source: source, backend: backend, adaptor: adaptor}
  end

  test "plain ingest", %{adaptor: adaptor, source: source} do
    log_event = build(:log_event, source: source, test: "data")

    Logflare.Google.BigQuery
    |> expect(:stream_batch!, fn _, _ ->
      {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
    end)

    assert [%{data: log_event}] == @subject.ingest(adaptor, [%{data: log_event}])
  end

  test "update table", %{adaptor: adaptor, source: source} do
    log_event = build(:log_event, lock_schema: false, source: source, test: "data")

    Logflare.Google.BigQuery
    |> stub(:stream_batch!, fn _, _ ->
      {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
    end)

    Logflare.Source.BigQuery.Schema
    |> expect(:update, fn _, _ -> :ok end)

    assert [%{data: log_event}] == @subject.ingest(adaptor, [%{data: log_event}])
  end
end
