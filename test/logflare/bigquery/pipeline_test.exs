defmodule Logflare.BigQuery.PipelineTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Sources.Source.BigQuery.Pipeline
  alias Logflare.LogEvent
  alias GoogleApi.BigQuery.V2.Model.TableDataInsertAllRequestRows
  alias Logflare.Backends.AdaptorSupervisor
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.Backend
  alias Logflare.Backends
  use ExUnitProperties

  @pipeline_name :test_pipeline
  describe "pipeline" do
    setup do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user_id: user.id)
      {:ok, source: source}
    end

    test "ack will remove items from pipeline if average rate is above 100", %{source: source} do
      sid_bid_pid = {source.id, nil, self()}
      IngestEventQueue.upsert_tid(sid_bid_pid)
      le = build(:log_event)
      IngestEventQueue.add_to_table(sid_bid_pid, [le])
      ref = {sid_bid_pid, source.token}
      message = Pipeline.transform(le, ref: ref)
      {mod, ref, _data} = message.acknowledger
      assert IngestEventQueue.get_table_size(sid_bid_pid) == 1
      mod.ack(ref, [message], [])
      refute IngestEventQueue.get_table_size(sid_bid_pid) == 0

      Logflare.PubSubRates.Cache.cache_rates(source.token, %{
        Node.self() => %{
          average_rate: 500,
          last_rate: 500,
          max_rate: 500,
          limiter_metrics: %{
            average: 0,
            duration: 60,
            sum: 0
          }
        }
      })

      mod.ack(ref, [message], [])
      assert IngestEventQueue.get_table_size(sid_bid_pid) == 0
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

  describe "bq_batch_size_splitter/2" do
    property "fallback inspect_payload/1 usage always overstates json encoded length" do
      check all payload <-
                  map_of(
                    string(:alphanumeric, min_length: 1),
                    string(:ascii, max_length: 1_000_000),
                    min_length: 20,
                    max_length: 500
                  ) do
        assert IO.iodata_length(Jason.encode!(payload)) <
                 Pipeline.message_size(payload)
      end
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
