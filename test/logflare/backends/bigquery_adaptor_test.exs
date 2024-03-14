defmodule Logflare.Backends.BigQueryAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.SourceSup

  alias Logflare.Sources.Counters
  alias Logflare.Sources.RateCounters
  alias Logflare.SystemMetrics.AllLogsLogged

  @subject Logflare.Backends.Adaptor.BigQueryAdaptor

  doctest @subject

  setup do
    start_supervised!(AllLogsLogged)
    start_supervised!(Counters)
    start_supervised!(RateCounters)

    Goth
    |> stub(:fetch, fn _mod -> {:ok, %Goth.Token{token: "auth-token"}} end)

    :ok
  end

  describe "default bigquery backend" do
    setup do
      insert(:plan)
      :ok
    end

    test "can ingest into source without creating a BQ backend" do
      user = insert(:user)
      source = insert(:source, user: user)
      start_supervised!({SourceSup, source})
      log_event = build(:log_event, source: source)

      Logflare.Google.BigQuery
      |> expect(:stream_batch!, fn _, _ ->
        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      assert :ok = Backends.ingest_logs([log_event], source)

      :timer.sleep(2000)
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

      :timer.sleep(2500)
      assert_received :ok
    end
  end

  describe "custom bigquery backend" do
    setup do
      insert(:plan)

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

      Logflare.Google.BigQuery
      |> expect(:stream_batch!, fn _, _ ->
        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      assert :ok =
               @subject.ingest(adaptor, [log_event], source_id: source.id, backend_id: backend.id)

      :timer.sleep(2500)
    end

    test "update table", %{adaptor: adaptor, source: source, backend: backend} do
      log_event = build(:log_event, source: source, test: "data")

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
        {:ok, %{}}
      end)

      :timer.sleep(500)

      Logflare.Mailer
      |> stub(:deliver, fn _ -> :ok end)

      assert :ok =
               @subject.ingest(adaptor, [log_event], source_id: source.id, backend_id: backend.id)

      :timer.sleep(2500)
    end
  end
end
