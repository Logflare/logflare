defmodule Logflare.Backends.BigQueryAdaptorTest do
  use Logflare.DataCase
  use ExUnitProperties

  alias Logflare.Backends
  alias Logflare.Backends.SourceSup
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
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

      TestUtils.retry_assert(fn ->
        assert_receive :streamed, 2500
      end)

      :timer.sleep(1000)
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

      TestUtils.retry_assert(fn ->
        assert_receive :ok, 2500
      end)

      :timer.sleep(1000)
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
      :timer.sleep(1000)
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
      :timer.sleep(1000)
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

      assert Backends.get_and_cache_local_pending_buffer_len(source.id, nil) == 1
      assert Backends.get_and_cache_local_pending_buffer_len(source.id, backend.id) == 1
      :timer.sleep(2000)

      TestUtils.retry_assert(fn ->
        assert_receive ^ref
      end)

      assert Backends.get_and_cache_local_pending_buffer_len(source.id, nil) == 0
      assert Backends.get_and_cache_local_pending_buffer_len(source.id, backend.id) == 0
    end
  end

  describe "handle_resolve_count/3" do
    test "resolve_count will increase counts when queue size is above threshold" do
      check all pipeline_count <- integer(0..100),
                queue_size <- integer(505..10000),
                avg_rate <- integer(100..10_000),
                last <- member_of([nil, NaiveDateTime.utc_now()]) do
        state = %{
          pipeline_count: pipeline_count,
          max_pipelines: 101,
          last_count_increase: last,
          last_count_decrease: last
        }

        desired =
          BigQueryAdaptor.handle_resolve_count(
            state,
            %{
              {1, 2, nil} => 0,
              {1, 2, make_ref()} => queue_size
            },
            avg_rate
          )

        assert desired > pipeline_count
      end
    end

    test "resolve_count will increase counts when startup queue is non-empty" do
      check all pipeline_count <- integer(0..100),
                queue_size <- integer(1..250),
                startup_queue_size <- integer(5000..10000),
                avg_rate <- integer(100..10_000),
                last <- member_of([nil, NaiveDateTime.utc_now()]) do
        state = %{
          pipeline_count: pipeline_count,
          max_pipelines: 101,
          last_count_increase: last,
          last_count_decrease: last
        }

        desired =
          BigQueryAdaptor.handle_resolve_count(
            state,
            %{
              {1, 2, nil} => startup_queue_size,
              {1, 2, make_ref()} => queue_size
            },
            avg_rate
          )

        assert desired - pipeline_count > 5
      end
    end

    test "resolve_count increases startup queue by 1 if less than 500 " do
      check all pipeline_count <- constant(0),
                startup_queue_size <- integer(1..444),
                avg_rate <- integer(1..500) do
        state = %{
          pipeline_count: pipeline_count,
          max_pipelines: 101,
          last_count_increase: NaiveDateTime.utc_now(),
          last_count_decrease: NaiveDateTime.utc_now()
        }

        desired =
          BigQueryAdaptor.handle_resolve_count(
            state,
            %{
              {1, 2, nil} => startup_queue_size
            },
            avg_rate
          )

        assert desired - pipeline_count == 1
      end
    end

    test "resolve_count will decrease counts" do
      check all pipeline_count <- integer(2..100),
                queue_size <- integer(0..49),
                startup_queue_size <- constant(0),
                avg_rate <- integer(0..10_000),
                since <- integer(71..100) do
        state = %{
          pipeline_count: pipeline_count,
          max_pipelines: 101,
          last_count_increase: NaiveDateTime.utc_now(),
          last_count_decrease: NaiveDateTime.utc_now() |> NaiveDateTime.add(-since)
        }

        desired =
          BigQueryAdaptor.handle_resolve_count(
            state,
            %{
              {1, 2, nil} => startup_queue_size,
              {1, 2, make_ref()} => queue_size
            },
            avg_rate
          )

        assert desired < pipeline_count
        assert desired != 0
      end
    end

    test "resolve_count scale to zero" do
      check all pipeline_count <- constant(1),
                queue_size <- constant(0),
                startup_queue_size <- constant(0),
                avg_rate <- constant(0),
                since <- integer(360..1000) do
        state = %{
          pipeline_count: pipeline_count,
          max_pipelines: 101,
          last_count_increase: NaiveDateTime.utc_now(),
          last_count_decrease: NaiveDateTime.utc_now() |> NaiveDateTime.add(-since)
        }

        desired =
          BigQueryAdaptor.handle_resolve_count(
            state,
            %{
              {1, 2, nil} => startup_queue_size,
              {1, 2, make_ref()} => queue_size
            },
            avg_rate
          )

        assert desired < pipeline_count
        assert desired == 0
      end
    end
  end
end
