defmodule Logflare.BigQuery.PipelineTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Source.BigQuery.Pipeline
  alias Logflare.LogEvent
  alias GoogleApi.BigQuery.V2.Model.TableDataInsertAllRequestRows
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
        rows = Map.get(opts[:body], :rows)
        BencheeAsync.Reporter.record(length(rows))
        # simulate some latency
        # :timer.sleep(100)
        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      user = insert(:user)
      source = insert(:source, user_id: user.id)
      args = [source: source, name: @pipeline_name]
      le = build(:log_event, source: source)

      batch =
        for _i <- 1..250 do
          %Broadway.Message{
            data: le,
            acknowledger: {__MODULE__, :ack_id, :ack_data}
          }
        end

      [batch: batch, args: args]
    end

    @tag :benchmark
    test "2-3", %{args: args, batch: batch, test: name} do
      start_supervised!(%{
        id: :something,
        start:
          {Pipeline, :start_link,
           [
             args,
             [
               processors: [default: [concurrency: 2]],
               batchers: [
                 bq: [
                   concurrency: 3,
                   batch_size: 250,
                   batch_timeout: 1_500
                 ]
               ]
             ]
           ]}
      })

      run_pipeline_benchmark(name, batch)
    end

    @tag :benchmark
    test "4-4", %{args: args, batch: batch, test: name} do
      start_supervised!(%{
        id: :something,
        start:
          {Pipeline, :start_link,
           [
             args,
             [
               processors: [default: [concurrency: 4]],
               batchers: [
                 bq: [
                   concurrency: 4,
                   batch_size: 250,
                   batch_timeout: 1_500
                 ]
               ]
             ]
           ]}
      })

      run_pipeline_benchmark(name, batch)
    end

    @tag :benchmark
    test "4-5", %{args: args, batch: batch, test: name} do
      start_supervised!(%{
        id: :something,
        start:
          {Pipeline, :start_link,
           [
             args,
             [
               processors: [default: [concurrency: 4]],
               batchers: [
                 bq: [
                   concurrency: 5,
                   batch_size: 250,
                   batch_timeout: 1_500
                 ]
               ]
             ]
           ]}
      })

      run_pipeline_benchmark(name, batch)
    end

    @tag :benchmark
    test "6-4", %{args: args, batch: batch, test: name} do
      start_supervised!(%{
        id: :something,
        start:
          {Pipeline, :start_link,
           [
             args,
             [
               processors: [default: [concurrency: 6]],
               batchers: [
                 bq: [
                   concurrency: 4,
                   batch_size: 250,
                   batch_timeout: 1_500
                 ]
               ]
             ]
           ]}
      })

      run_pipeline_benchmark(name, batch)
    end

    @tag :benchmark
    test "6-5", %{args: args, batch: batch, test: name} do
      start_supervised!(%{
        id: :something,
        start:
          {Pipeline, :start_link,
           [
             args,
             [
               processors: [default: [concurrency: 6]],
               batchers: [
                 bq: [
                   concurrency: 5,
                   batch_size: 250,
                   batch_timeout: 1_500
                 ]
               ]
             ]
           ]}
      })

      run_pipeline_benchmark(name, batch)
    end

    @tag :benchmark
    test "6-6", %{args: args, batch: batch, test: name} do
      start_supervised!(%{
        id: :something,
        start:
          {Pipeline, :start_link,
           [
             args,
             [
               processors: [default: [concurrency: 6]],
               batchers: [
                 bq: [
                   concurrency: 5,
                   batch_size: 250,
                   batch_timeout: 1_500
                 ]
               ]
             ]
           ]}
      })

      run_pipeline_benchmark(name, batch)
    end

    @tag :benchmark
    test "8-4 - best", %{args: args, batch: batch, test: name} do
      start_supervised!(%{
        id: :something,
        start:
          {Pipeline, :start_link,
           [
             args,
             [
               processors: [default: [concurrency: 8]],
               batchers: [
                 bq: [
                   concurrency: 4,
                   batch_size: 250,
                   batch_timeout: 1_500
                 ]
               ]
             ]
           ]}
      })

      run_pipeline_benchmark(name, batch)
    end

    @tag :benchmark
    test "10-5", %{args: args, batch: batch, test: name} do
      start_supervised!(%{
        id: :something,
        start:
          {Pipeline, :start_link,
           [
             args,
             [
               processors: [default: [concurrency: 6]],
               batchers: [
                 bq: [
                   concurrency: 5,
                   batch_size: 250,
                   batch_timeout: 1_500
                 ]
               ]
             ]
           ]}
      })

      run_pipeline_benchmark(name, batch)
    end

    @tag :benchmark
    test "10-8", %{args: args, batch: batch, test: name} do
      start_supervised!(%{
        id: :something,
        start:
          {Pipeline, :start_link,
           [
             args,
             [
               processors: [default: [concurrency: 6]],
               batchers: [
                 bq: [
                   concurrency: 8,
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
  end

  def ack(_ack_ref, _successful, _failed) do
  end
end
