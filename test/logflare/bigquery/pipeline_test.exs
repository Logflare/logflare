defmodule Logflare.BigQuery.PipelineTest do
  @moduledoc false
  use Logflare.DataCase
  use ExUnitProperties

  import ExUnit.CaptureLog

  alias Broadway.Message
  alias GoogleApi.BigQuery.V2.Model.TableDataInsertAllRequestRows
  alias Logflare.Backends
  alias Logflare.Backends.AdaptorSupervisor
  alias Logflare.Backends.Backend
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.LogEvent
  alias Logflare.Repo
  alias Logflare.Sources.Source.BigQuery.Pipeline
  alias Logflare.User

  @pipeline_name :test_pipeline
  describe "pipeline" do
    setup do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user_id: user.id)
      {:ok, source: source}
    end

    test "ack will requeue failed events", %{source: source} do
      sid_bid_pid = {source.id, nil, self()}
      IngestEventQueue.upsert_tid(sid_bid_pid)
      le = build(:log_event)
      # Add event to ETS and mark as :processing (simulating it was taken by the producer)
      IngestEventQueue.add_to_table(sid_bid_pid, [le])
      tid = IngestEventQueue.get_tid(sid_bid_pid)
      IngestEventQueue.mark_ingested(sid_bid_pid, [le])

      ref = {sid_bid_pid, %{max_retries: 1}}
      message = Pipeline.transform({le.id, tid, 0}, ref: ref)
      {mod, ref, _data} = message.acknowledger
      assert IngestEventQueue.get_table_size(sid_bid_pid) == 1

      mod.ack(ref, [], [message])
      # Event is deleted then re-added as pending with incremented retries
      assert IngestEventQueue.total_pending(sid_bid_pid) == 1

      {:ok, [m]} = IngestEventQueue.pop_pending(sid_bid_pid, 1)

      assert m.retries == 1
    end

    test "ack will not requeue failed events that have exhausted retries", %{source: source} do
      sid_bid_pid = {source.id, nil, self()}
      IngestEventQueue.upsert_tid(sid_bid_pid)
      le = build(:log_event) |> Map.put(:retries, 1)
      # Add event to ETS and mark as :processing (simulating it was taken by the producer)
      IngestEventQueue.add_to_table(sid_bid_pid, [le])
      tid = IngestEventQueue.get_tid(sid_bid_pid)
      IngestEventQueue.mark_ingested(sid_bid_pid, [le])

      ref = {sid_bid_pid, %{max_retries: 1}}
      message = Pipeline.transform({le.id, tid, 0}, ref: ref)
      {mod, ref, _data} = message.acknowledger
      assert IngestEventQueue.get_table_size(sid_bid_pid) == 1

      mod.ack(ref, [], [message])

      # Event is NOT requeued (retries == max_retries); deleted from ETS
      assert IngestEventQueue.total_pending(sid_bid_pid) == 0
      assert IngestEventQueue.get_table_size(sid_bid_pid) == 0
    end

    test "ack deletes events directly when avg > 100", %{source: source} do
      stub(Logflare.Sources, :get_source_metrics_for_ingest, fn _ -> %{avg: 200} end)

      sid_bid_pid = {source.id, nil, self()}
      IngestEventQueue.upsert_tid(sid_bid_pid)
      le = build(:log_event, source: source)
      IngestEventQueue.add_to_table(sid_bid_pid, [le])
      tid = IngestEventQueue.get_tid(sid_bid_pid)
      :ets.update_element(tid, le.id, {2, :processing})

      ref = {sid_bid_pid, %{max_retries: 0}}
      message = Pipeline.transform({le.id, tid, 0}, ref: ref)
      {mod, ref, _} = message.acknowledger

      mod.ack(ref, [message], [])

      assert IngestEventQueue.get_table_size(sid_bid_pid) == 0
    end

    test "ack marks events as :ingested when avg <= 100", %{source: source} do
      stub(Logflare.Sources, :get_source_metrics_for_ingest, fn _ -> %{avg: 50} end)

      sid_bid_pid = {source.id, nil, self()}
      IngestEventQueue.upsert_tid(sid_bid_pid)
      le = build(:log_event, source: source)
      IngestEventQueue.add_to_table(sid_bid_pid, [le])
      tid = IngestEventQueue.get_tid(sid_bid_pid)
      :ets.update_element(tid, le.id, {2, :processing})

      ref = {sid_bid_pid, %{max_retries: 0}}
      message = Pipeline.transform({le.id, tid, 0}, ref: ref)
      {mod, ref, _} = message.acknowledger

      mod.ack(ref, [message], [])

      assert IngestEventQueue.get_table_size(sid_bid_pid) == 1
      assert IngestEventQueue.total_pending(sid_bid_pid) == 0
      assert [{_id, :ingested, _, _, _}] = :ets.lookup(tid, le.id)
    end

    test "le_to_bq_row/1 generates TableDataInsertAllRequestRows struct correctly", %{
      source: source
    } do
      datetime = DateTime.utc_now()

      le =
        LogEvent.make(
          %{
            "event_message" => "valid",
            "top_level" => "top",
            "project" => "my-project",
            "metadata" => %{"a" => "nested"},
            "timestamp" => datetime |> DateTime.to_unix(:microsecond)
          },
          %{source: source}
        )

      id = le.id

      assert %TableDataInsertAllRequestRows{
               insertId: ^id,
               json: %{
                 "event_message" => "valid",
                 "top_level" => "top",
                 "timestamp" => ^datetime,
                 "metadata" => [%{"a" => "nested"}],
                 "id" => ^id,
                 "project" => "my-project"
               }
             } = Pipeline.le_to_bq_row(le)
    end
  end

  describe "le_to_bq_row/1 OpenTelemetry timestamp conversion" do
    setup do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user_id: user.id)
      {:ok, source: source}
    end

    @start_ns System.system_time(:nanosecond)
    @end_ns System.system_time(:nanosecond) + 1_000_000

    defp make_le(source, attrs) do
      build(:log_event, [source: source, message: "test"] ++ attrs)
    end

    defp bq_json(le), do: Pipeline.le_to_bq_row(le).json

    test "converts OTel timestamps from nanoseconds to microseconds", %{source: source} do
      json =
        make_le(source,
          resource: %{"service.name" => "svc"},
          scope: %{"name" => "scope"},
          start_time: @start_ns,
          end_time: @end_ns
        )
        |> bq_json()

      assert %DateTime{} = json["end_time"]
      assert %DateTime{} = json["start_time"]
    end

    test "converts only start_time when end_time is missing", %{source: source} do
      json =
        make_le(source,
          resource: %{"service.name" => "svc"},
          scope: %{"name" => "scope"},
          start_time: @start_ns
        )
        |> bq_json()

      assert is_nil(json["end_time"])
      assert %DateTime{} = json["start_time"]
    end

    test "does not convert timestamps without both resource and scope", %{source: source} do
      for attrs <- [
            [
              scope: %{"name" => "scope"}
            ],
            [
              resource: %{"service.name" => "svc"}
            ]
          ],
          attrs = attrs ++ [start_time: @start_ns, end_time: @end_ns] do
        json = make_le(source, attrs) |> bq_json()
        # should not be converted to microseconds
        refute match?(%DateTime{}, json["start_time"])
        refute match?(%DateTime{}, json["end_time"])
      end
    end
  end

  describe "disconnect_backend_and_email" do
    setup do
      insert(:plan)

      user =
        insert(:user,
          bigquery_project_id: "my-byob-project",
          bigquery_dataset_id: "my_dataset",
          bigquery_dataset_location: "US"
        )

      source = insert(:source, user_id: user.id)

      {:ok, user: user, source: source}
    end

    test "resets BQ settings on free tier streaming error", %{user: user, source: source} do
      error_body =
        Jason.encode!(%{
          "error" => %{
            "message" =>
              "Access Denied: BigQuery BigQuery: Streaming insert is not allowed in the free tier"
          }
        })

      stub(Logflare.Google.BigQuery, :stream_batch!, fn _context, _rows ->
        {:error, %Tesla.Env{body: error_body}}
      end)

      expect(Logflare.Sources.Source.Supervisor, :reset_all_user_sources, fn _user -> :ok end)
      expect(Logflare.Mailer, :deliver, fn _email -> {:ok, %{}} end)

      context = %{
        source_token: source.token,
        user_id: user.id,
        system_source: false,
        bigquery_project_id: user.bigquery_project_id,
        bigquery_dataset_id: user.bigquery_dataset_id
      }

      log =
        capture_log([level: :warning], fn ->
          Pipeline.stream_batch(context, [{build(:log_event, source: source), 0}])
        end)

      assert log =~ "user audit: BigQuery backend auto-disconnect triggered"
      assert log =~ "user audit: BigQuery backend auto-disconnected"

      updated_user = Repo.get!(User, user.id)
      assert is_nil(updated_user.bigquery_project_id)
      assert is_nil(updated_user.bigquery_dataset_id)
      assert is_nil(updated_user.bigquery_dataset_location)
      assert updated_user.bigquery_processed_bytes_limit == 10_000_000_000
    end

    test "resets BQ settings on project not enabled error", %{user: user, source: source} do
      error_body =
        Jason.encode!(%{
          "error" => %{
            "message" => "The project my-byob-project has not enabled BigQuery."
          }
        })

      stub(Logflare.Google.BigQuery, :stream_batch!, fn _context, _rows ->
        {:error, %Tesla.Env{body: error_body}}
      end)

      expect(Logflare.Sources.Source.Supervisor, :reset_all_user_sources, fn _user -> :ok end)
      expect(Logflare.Mailer, :deliver, fn _email -> {:ok, %{}} end)

      context = %{
        source_token: source.token,
        user_id: user.id,
        system_source: false,
        bigquery_project_id: user.bigquery_project_id,
        bigquery_dataset_id: user.bigquery_dataset_id
      }

      log =
        capture_log([level: :warning], fn ->
          Pipeline.stream_batch(context, [{build(:log_event, source: source), 0}])
        end)

      assert log =~ "user audit: BigQuery backend auto-disconnect triggered"
      assert log =~ "user audit: BigQuery backend auto-disconnected"

      updated_user = Repo.get!(User, user.id)
      assert is_nil(updated_user.bigquery_project_id)
      assert is_nil(updated_user.bigquery_dataset_id)
      assert is_nil(updated_user.bigquery_dataset_location)
    end

    test "does not reset BQ settings on other errors", %{user: user, source: source} do
      error_body =
        Jason.encode!(%{
          "error" => %{"message" => "Some transient error"}
        })

      stub(Logflare.Google.BigQuery, :stream_batch!, fn _context, _rows ->
        {:error, %Tesla.Env{body: error_body}}
      end)

      context = %{
        source_token: source.token,
        user_id: user.id,
        system_source: false,
        bigquery_project_id: user.bigquery_project_id,
        bigquery_dataset_id: user.bigquery_dataset_id
      }

      capture_log([level: :warning], fn ->
        Pipeline.stream_batch(context, [])
      end)

      updated_user = Repo.get!(User, user.id)
      assert updated_user.bigquery_project_id == "my-byob-project"
      assert updated_user.bigquery_dataset_id == "my_dataset"
      assert updated_user.bigquery_dataset_location == "US"
    end
  end

  describe "benchmarks" do
    setup do
      start_supervised!(BencheeAsync.Reporter)

      GoogleApi.BigQuery.V2.Api.Tabledata
      |> stub(:bigquery_tabledata_insert_all, fn _conn,
                                                 _project_id,
                                                 _dataset_id,
                                                 _table_name,
                                                 opts ->
        :timer.sleep(10)
        rows = Map.get(opts[:body], :rows)
        BencheeAsync.Reporter.record(length(rows))
        # simulate some latency
        # :timer.sleep(100)
        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user, lock_schema: true)
      args = [source: source, backend: Backends.get_default_backend(user), name: @pipeline_name]
      le = build(:log_event, source: source)

      start_supervised!(
        {AdaptorSupervisor,
         {source, %Backend{type: :bigquery, config: %{project_id: nil, dataset_id: nil}}}}
      )

      batch =
        for _i <- 1..250 do
          %Broadway.Message{
            data: le,
            acknowledger: {__MODULE__, :ack_id, :ack_data}
          }
        end

      [batch: batch, args: args]
    end

    for {processors, batchers} <- [
          # {4, 4}, #264k, 33k/proc
          # {4, 6}, #368k, 36.8k/proc
          # 472k, 39k/proc
          # {4, 8},
          # 559k, 39k/proc
          # {4, 10},
          # {4, 12},
          # {4, 14},
          # 527.75k
          {4, 16},
          # {6, 6},
          # {6, 8},
          # {6, 10},
          # {6, 12},
          # {6, 14},
          # {6, 16},
          # {8, 8},
          # 515.25k-539.00k
          {8, 12},
          # 696.75k-778.75k
          {8, 16}
          # {12, 12}, #544.50
          # {12, 16}, #855.75k
        ] do
      @tag :benchmark
      test "#{processors}-#{batchers}", %{args: args, batch: batch, test: name} do
        start_supervised!(%{
          id: :something,
          start:
            {Pipeline, :start_link,
             [
               args,
               [
                 processors: [default: [concurrency: unquote(processors)]],
                 batchers: [
                   bq: [
                     concurrency: unquote(batchers),
                     batch_size: 250,
                     batch_timeout: 1_500
                   ]
                 ]
               ]
             ]}
        })

        run_pipeline_benchmark(name, batch)
      end
    end
  end

  describe "handle_batch/4" do
    setup do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user_id: user.id)

      context = %{
        source_id: source.id,
        source_token: source.token,
        backend_id: nil,
        bigquery_project_id: nil,
        bigquery_dataset_id: nil,
        user_id: user.id,
        system_source: false
      }

      batch_info = %Broadway.BatchInfo{batcher: :bq, batch_key: :bq, size: 1, trigger: :flush}

      {:ok, source: source, context: context, batch_info: batch_info}
    end

    defp setup_queue(source, events) do
      sid_bid_pid = {source.id, nil, self()}
      IngestEventQueue.upsert_tid(sid_bid_pid)
      IngestEventQueue.add_to_table(sid_bid_pid, events)
      {:ok, id_size_pairs, tid} = IngestEventQueue.take_pending_ids(sid_bid_pid, length(events))

      messages =
        Enum.map(id_size_pairs, fn {id, size} ->
          %Message{data: {id, tid, size}, acknowledger: {Pipeline, :ack_id, :ack_data}}
        end)

      {messages, tid}
    end

    test "passes {id, tid, size} messages through with correct size", %{
      source: source,
      context: context,
      batch_info: batch_info
    } do
      stub(Logflare.Google.BigQuery, :stream_batch!, fn _ctx, _rows ->
        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      le = build(:log_event, source: source)
      {[message], tid} = setup_queue(source, [le])

      [result] = Pipeline.handle_batch(:bq, [message], batch_info, context)

      assert {id, ^tid, size} = result.data
      assert id == le.id
      assert is_integer(size) and size > 0
    end

    test "excludes missing IDs and emits telemetry", %{
      source: source,
      context: context,
      batch_info: batch_info
    } do
      stub(Logflare.Google.BigQuery, :stream_batch!, fn _ctx, _rows ->
        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      le = build(:log_event, source: source)
      {[message], tid} = setup_queue(source, [le])

      # Delete event from ETS so ID cannot be resolved
      :ets.delete(tid, le.id)

      ref = make_ref()

      :telemetry.attach(
        "test-missing-#{inspect(ref)}",
        [:logflare, :ingest_event_queue, :missing_ids],
        fn _event, %{count: n}, _meta, pid -> send(pid, {:missing, n}) end,
        self()
      )

      [result_msg] = Pipeline.handle_batch(:bq, [message], batch_info, context)

      # Missing message is returned as failed (not dropped) so Broadway can ack it
      assert {:failed, "missing from ETS"} = result_msg.status
      assert {_id, _tid, 0} = result_msg.data
      assert_receive {:missing, 1}

      :telemetry.detach("test-missing-#{inspect(ref)}")
    end

    test "sends all events in a single stream_batch! call when under size limit", %{
      source: source,
      context: context,
      batch_info: batch_info
    } do
      les = for _ <- 1..3, do: build(:log_event, source: source)
      {messages, _tid} = setup_queue(source, les)

      expect(Logflare.Google.BigQuery, :stream_batch!, 1, fn _ctx, rows ->
        assert length(rows) == 3
        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      Pipeline.handle_batch(:bq, messages, batch_info, context)
    end

    test "calls stream_batch! with resolved log events", %{
      source: source,
      context: context,
      batch_info: batch_info
    } do
      le = build(:log_event, source: source)
      {messages, _tid} = setup_queue(source, [le])

      expect(Logflare.Google.BigQuery, :stream_batch!, 1, fn _ctx, rows ->
        ids = Enum.map(rows, & &1.insertId)
        assert le.id in ids
        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      Pipeline.handle_batch(:bq, messages, batch_info, context)
    end

    test "calls insert_log_events_via_storage_write_api on storage write path", %{
      source: source,
      batch_info: batch_info
    } do
      source = insert(:source, user_id: source.user_id, bq_storage_write_api: true)

      context = %{
        source_id: source.id,
        source_token: source.token,
        backend_id: nil,
        bigquery_project_id: nil,
        bigquery_dataset_id: nil,
        user_id: source.user_id,
        system_source: false
      }

      le = build(:log_event, source: source)
      {messages, _tid} = setup_queue(source, [le])

      expect(BigQueryAdaptor, :insert_log_events_via_storage_write_api, 1, fn log_events, _opts ->
        ids = Enum.map(log_events, & &1.id)
        assert le.id in ids
        :ok
      end)

      Pipeline.handle_batch(:bq, messages, batch_info, context)
    end
  end

  defp run_pipeline_benchmark(name, batch) do
    BencheeAsync.run(
      %{
        inspect(name) => fn ->
          Broadway.push_messages(@pipeline_name, batch)
        end
      },
      time: 3,
      warmup: 1,
      print: [configuration: false],
      # use extended_statistics to view units of work done
      formatters: [{Benchee.Formatters.Console, extended_statistics: true}]
    )

    :timer.sleep(1000)
  end

  def ack(_ack_ref, _successful, _failed) do
  end
end
