defmodule Logflare.BigQuery.PipelineTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Source.BigQuery.Pipeline
  alias Logflare.LogEvent
  alias GoogleApi.BigQuery.V2.Model.TableDataInsertAllRequestRows
  alias Logflare.Backends.AdaptorSupervisor
  alias Logflare.Backends.Backend
  use ExUnitProperties

  @pipeline_name :test_pipeline
  describe "pipeline" do
    setup do
      user = insert(:user)
      source = insert(:source, user_id: user.id)
      {:ok, source: source}
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
                 IO.iodata_length(Pipeline.inspect_payload(payload))
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
      source = insert(:source, user_id: user.id, lock_schema: true)
      args = [source: source, name: @pipeline_name]
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
          {4, 8},
          # 559k, 39k/proc
          {4, 10}
          # {4, 16},#743k,  37k/proc
          # {6, 6}, #395k, 32.9k/proc
          # {6, 8}, #500k, 35.7k/proc
          # {6, 10}, #595k, 37.1k/proc
          # {6, 12},#680k, 37k/proc
          # {6, 14},#757.75k, 37.8k/proc
          # {6, 16}, #813.25k, 36.96/proc
          # {8, 8}, #522, 32k/proc
          # {8, 16},#856k, 35k/proc
          # {12, 8}, #525k, 26k/proc
          # {10, 16}, #907k, 34k/proc
          # {12, 16}, #953k, 34k/proc
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
