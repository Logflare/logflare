defmodule Logflare.BigQuery.PipelineTest do
  @moduledoc false
  alias Logflare.Source.BigQuery.Pipeline
  alias Logflare.{LogEvent}
  alias GoogleApi.BigQuery.V2.Model.TableDataInsertAllRequestRows
  alias Logflare.Source.RecentLogsServer, as: RLS
  use Logflare.DataCase

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

  describe "benchmarks" do
    setup do
      start_supervised!(BencheeAsync.Reporter)

      GoogleApi.BigQuery.V2.Api.Tabledata
      |> stub(:bigquery_tabledata_insert_all, fn _conn,
                                                 _project_id,
                                                 _dataset_id,
                                                 _table_name,
                                                 _opts ->
        BencheeAsync.Reporter.record()
        # simulate some latency
        # :timer.sleep(100)
        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      user = insert(:user)
      source = insert(:source, user_id: user.id)
      rls = %RLS{source_id: source.token, source: source}
      le = build(:log_event, source: source)

      batch =
        for _i <- 1..250 do
          %Broadway.Message{
            data: le,
            acknowledger: {__MODULE__, :ack_id, :ack_data}
          }
        end

      [batch: batch, rls: rls]
    end

    @tag :benchmark
    test "defaults", %{test: name, rls: rls, batch: batch} do
      start_supervised!({Pipeline, [rls, name: @pipeline_name]})
      run_pipeline_benchmark(name, batch)
    end

    @tag :benchmark
    test "schedulers_online", %{test: name, rls: rls, batch: batch} do
      start_supervised!(
        {Pipeline,
         [
           rls,
           name: @pipeline_name,
           processors: [default: [concurrency: System.schedulers_online()]]
         ]}
      )

      run_pipeline_benchmark(name, batch)
    end

    @tag :benchmark
    @tag :skip
    test "schedulers_online, max_demand=250", %{rls: rls, batch: batch, test: name} do
      start_supervised!(
        {Pipeline,
         [
           rls,
           name: @pipeline_name,
           processors: [default: [concurrency: System.schedulers_online(), max_demand: 250]]
         ]}
      )

      run_pipeline_benchmark(name, batch)
    end

    @tag :benchmark
    test "schedulers_online x2", %{rls: rls, batch: batch, test: name} do
      start_supervised!(
        {Pipeline,
         [
           rls,
           name: @pipeline_name,
           processors: [default: [concurrency: System.schedulers_online() * 2]]
         ]}
      )

      run_pipeline_benchmark(name, batch)
    end

    @tag :benchmark
    test "schedulers_online, batchers schedulers x2", %{rls: rls, batch: batch, test: name} do
      start_supervised!(
        {Pipeline,
         [
           rls,
           name: @pipeline_name,
           processors: [default: [concurrency: System.schedulers_online()]],
           batchers: [
             bq: [
               concurrency: System.schedulers_online() * 2,
               batch_size: 250,
               batch_timeout: 1_500
             ]
           ]
         ]}
      )

      run_pipeline_benchmark(name, batch)
    end

    @tag :benchmark
    test "schedulers_online x2, batchers schedulers x2", %{rls: rls, batch: batch, test: name} do
      start_supervised!(
        {Pipeline,
         [
           rls,
           name: @pipeline_name,
           processors: [default: [concurrency: System.schedulers_online() * 2]],
           batchers: [
             bq: [
               concurrency: System.schedulers_online() * 2,
               batch_size: 250,
               batch_timeout: 1_500
             ]
           ]
         ]}
      )

      run_pipeline_benchmark(name, batch)
    end

    @tag :benchmark
    test "schedulers_online x2, batchers schedulers x3", %{rls: rls, batch: batch, test: name} do
      start_supervised!(
        {Pipeline,
         [
           rls,
           name: @pipeline_name,
           processors: [default: [concurrency: System.schedulers_online() * 2]],
           batchers: [
             bq: [
               concurrency: System.schedulers_online() * 3,
               batch_size: 250,
               batch_timeout: 1_500
             ]
           ]
         ]}
      )

      run_pipeline_benchmark(name, batch)
    end

    @tag :benchmark
    test "schedulers_online x3, batchers schedulers x3", %{rls: rls, batch: batch, test: name} do
      start_supervised!(
        {Pipeline,
         [
           rls,
           name: @pipeline_name,
           processors: [default: [concurrency: System.schedulers_online() * 3]],
           batchers: [
             bq: [
               concurrency: System.schedulers_online() * 3,
               batch_size: 250,
               batch_timeout: 1_500
             ]
           ]
         ]}
      )

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
