defmodule Logflare.Backends.BigQueryAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.SourceSup
  alias Logflare.SystemMetrics.AllLogsLogged

  @subject Logflare.Backends.Adaptor.BigQueryAdaptor

  doctest @subject

  setup do
    start_supervised!(AllLogsLogged)
    insert(:plan)
    :ok
  end

  describe "default bigquery backend" do
    test "can ingest into source without creating a BQ backend" do
      user = insert(:user)
      source = insert(:source, user: user)
      start_supervised!({SourceSup, source})
      log_event = build(:log_event, source: source)
      pid = self()

      Logflare.Google.BigQuery
      |> expect(:stream_batch!, fn _, _ ->
        send(pid, :streamed)
        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      assert :ok = Backends.ingest_logs([log_event], source)
      assert_receive :streamed, 2500
    end

    test "does not use LF managed BQ if legacy user BQ config is set" do
      user = insert(:user, bigquery_project_id: "some-project", bigquery_dataset_id: "some-id")
      source = insert(:source, user: user)
      start_supervised!({SourceSup, source})
      log_event = build(:log_event, source: source)

      pid = self()

      Logflare.Google.BigQuery
      |> expect(:stream_batch!, fn arg, _ ->
        assert arg.bigquery_project_id == "some-project"
        assert arg.bigquery_dataset_id == "some-id"
        send(pid, :ok)
        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      assert :ok = Backends.ingest_logs([log_event], source)

      assert_receive :ok, 2500
    end
  end

  describe "custom bigquery backend" do
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

      adaptor = start_supervised!(Adaptor.child_spec(source, backend))

      {:ok, source: source, backend: backend, adaptor: adaptor}
    end

    test "plain ingest", %{adaptor: adaptor, source: source, backend: backend} do
      log_event = build(:log_event, source: source)
      pid = self()

      Logflare.Google.BigQuery
      |> expect(:stream_batch!, fn _, _ ->
        send(pid, :streamed)
        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      assert :ok =
               @subject.ingest(adaptor, [log_event], source_id: source.id, backend_id: backend.id)

      assert_receive :streamed, 2500
    end

    test "update table", %{adaptor: adaptor, source: source, backend: backend} do
      log_event = build(:log_event, source: source, test: "data")
      pid = self()

      Logflare.Google.BigQuery
      |> stub(:stream_batch!, fn _, _ ->
        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      GoogleApi.BigQuery.V2.Api.Tables
      |> expect(:bigquery_tables_patch, fn _conn,
                                           _project_id,
                                           _dataset_id,
                                           _table_name,
                                           [body: _body] ->
        send(pid, :patched)
        {:ok, %{}}
      end)

      Logflare.Mailer
      |> stub(:deliver, fn _ -> :ok end)

      assert :ok =
               @subject.ingest(adaptor, [log_event], source_id: source.id, backend_id: backend.id)

      assert_receive :patched, 2500
    end
  end
end
