defmodule Logflare.Backends.BigQueryAdaptorTest do
  use Logflare.DataCase

  alias Logflare.Backends
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

      assert {:ok, 1} = Backends.ingest_logs([log_event], source)
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

      assert {:ok, 1} = Backends.ingest_logs([log_event], source)

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

      start_supervised!({SourceSup, source})

      {:ok, source: source, backend: backend}
    end

    test "plain ingest", %{source: source} do
      log_event = build(:log_event, source: source)
      pid = self()

      Logflare.Google.BigQuery
      |> expect(:stream_batch!, fn _, _ ->
        send(pid, :streamed)
        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      assert {:ok, _} = Backends.ingest_logs([log_event], source)

      assert_receive :streamed, 2500
    end

    test "update table", %{source: source} do
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

      assert {:ok, _} = Backends.ingest_logs([log_event], source)

      assert_receive :patched, 2500
    end

    test "bug: invalid json encode update table", %{
      source: source,
      backend: backend
    } do
      log_event = build(:log_event, source: source, test: <<97, 98, 99, 222, 126, 199, 31, 89>>)
      pid = self()
      ref = make_ref()

      Logflare.Google.BigQuery
      |> stub(:stream_batch!, fn _, _ ->
        send(pid, ref)
        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      GoogleApi.BigQuery.V2.Api.Tables
      |> stub(:bigquery_tables_patch, fn _conn,
                                         _project_id,
                                         _dataset_id,
                                         _table_name,
                                         [body: _body] ->
        {:ok, %{}}
      end)

      Logflare.Mailer
      |> stub(:deliver, fn _ -> :ok end)

      assert {:ok, _} = Backends.ingest_logs([log_event], source)

      assert Backends.local_pending_buffer_len(source, nil) == 1
      assert Backends.local_pending_buffer_len(source, backend) == 1
      :timer.sleep(2000)
      assert_receive ^ref
      assert Backends.local_pending_buffer_len(source, nil) == 0
      assert Backends.local_pending_buffer_len(source, backend) == 0
    end
  end

  describe "dynamic sharding of pipeline" do
    setup do
      config = %{
        project_id: "some-project",
        dataset_id: "some-dataset"
      }

      backend =
        insert(:backend,
          type: :bigquery,
          config: config
        )

      source = insert(:source, user: insert(:user), backends: [backend])

      start_supervised!({SourceSup, source})

      Broadway
      |> stub(:push_messages, fn _, _ -> :ok end)

      [source: source, backend: backend]
    end

    test "when buffer at >80% capacity, pipeline gets sharded automatically", %{
      source: source,
      backend: backend
    } do
      le = build(:log_event, source: source)
      batch = for _i <- 1..1100, do: le
      assert {:ok, 1100} = Backends.ingest_logs(batch, source)
      assert {:ok, 1100} = Backends.ingest_logs(batch, source)
      assert {:ok, 1100} = Backends.ingest_logs(batch, source)
      assert @subject.buffer_capacity(source.id, backend.id) > 0.6
      assert {:ok, 1100} = Backends.ingest_logs(batch, source)
      assert {:ok, 1100} = Backends.ingest_logs(batch, source)
      :timer.sleep(500)
      assert @subject.buffer_capacity(source.id, backend.id) < 0.6
    end
  end
end
