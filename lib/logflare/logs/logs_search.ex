defmodule Logflare.Logs.Search do
  @moduledoc false

  alias Logflare.Logs.SearchOperation, as: SO
  import Logflare.Logs.SearchOperations
  alias Logflare.Logs.SearchQueries
  alias Logflare.Source
  alias Logflare.BqRepo
  alias Logflare.Google.BigQuery.GCPConfig
  import Ecto.Query

  def search_and_aggs(%SO{} = so) do
    tasks = [
      Task.async(fn -> search_events(so) end),
      Task.async(fn -> search_result_aggregates(so) end)
    ]

    tasks_with_results = Task.yield_many(tasks, 30_000)

    results =
      tasks_with_results
      |> Enum.map(fn {task, res} ->
        res || Task.shutdown(task, :brutal_kill)
      end)

    [event_result, agg_result] = results

    with {:ok, {:ok, events_so}} <- event_result,
         {:ok, {:ok, agg_so}} <- agg_result do
      {:ok, %{events: events_so, aggregates: agg_so}}
    else
      {:ok, {:error, result_so}} ->
        {:error, result_so}

      {:error, result_so} ->
        {:error, result_so}

      nil ->
        {:error, "Search task timeout"}
    end
  end

  def search_events(%SO{} = so) do
    so = %{so | type: :events} |> put_time_stats()

    with %{error: nil} = so <- apply_query_defaults(so),
         %{error: nil} = so <- apply_halt_conditions(so),
         %{error: nil} = so <- apply_local_timestamp_correction(so),
         %{error: nil} = so <- apply_timestamp_filter_rules(so),
         %{error: nil} = so <- apply_filters(so),
         %{error: nil} = so <- apply_to_sql(so),
         %{error: nil} = so <- do_query(so),
         %{error: nil} = so <- apply_warning_conditions(so),
         %{error: nil} = so <- put_stats(so) do
      {:ok, so}
    else
      so -> {:error, so}
    end
  end

  def search_result_aggregates(%SO{} = so) do
    so = %{so | type: :aggregates} |> put_time_stats()

    with %{error: nil} = so <- apply_halt_conditions(so),
         %{error: nil} = so <- put_chart_data_shape_id(so),
         %{error: nil} = so <- apply_local_timestamp_correction(so),
         %{error: nil} = so <- apply_timestamp_filter_rules(so),
         %{error: nil} = so <- apply_numeric_aggs(so),
         %{error: nil} = so <- apply_to_sql(so),
         %{error: nil} = so <- do_query(so),
         %{error: nil} = so <- process_query_result(so),
         %{error: nil} = so <- add_missing_agg_timestamps(so),
         %{error: nil} = so <- apply_warning_conditions(so),
         %{error: nil} = so <- put_stats(so) do
      {:ok, so}
    else
      so -> {:error, so}
    end
  end

  def query_source_streaming_buffer(%Source{} = source) do
    q =
      source.bq_table_id
      |> SearchQueries.source_table_streaming_buffer()
      |> order_by(desc: :timestamp)
      |> limit(100)

    bq_project_id = source.user.bigquery_project_id || GCPConfig.default_project_id()
    BqRepo.query(bq_project_id, q)
  end
end
