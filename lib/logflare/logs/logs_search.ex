defmodule Logflare.Logs.Search do
  @moduledoc false
  alias Logflare.Google.BigQuery.{GenUtils, SchemaUtils}
  alias Logflare.{Source, Sources, EctoQueryBQ}

  alias Logflare.Logs.Search.Parser
  import Ecto.Query

  alias GoogleApi.BigQuery.V2.Api
  alias GoogleApi.BigQuery.V2.Model.QueryRequest

  import Logflare.Logs.SearchOperations.Utils
  import Logflare.Logs.SearchOperations

  alias Logflare.Logs.SearchOperations.SearchOperation, as: SO

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

  @spec search_result_aggregates(SO.t()) :: {:ok, SO.t()} | {:error, SO.t()}
  def search_result_aggregates(%SO{} = so) do
    so
    |> do_search_without_select()
    |> exclude_limit()
    |> apply_group_by_timestamp_period()
    |> apply_numeric_aggs()
    |> apply_to_sql()
    |> do_query()
    |> process_agg_query_result()
    |> put_stats()
    |> case do
      %{error: nil} = so ->
        {:ok, so}

      %{error: e} = so when not is_nil(e) ->
        {:error, so}
    end
  end

  @spec search_events(SO.t()) :: {:ok, SO.t()} | {:error, SO.t()}
  def search_events(%SO{} = so) do
    so
    |> do_search_without_select()
    |> partition_or_streaming()
    |> order_by_default()
    |> apply_limit_to_query()
    |> apply_select_all_schema()
    |> apply_to_sql()
    |> do_query()
    |> process_query_result()
    |> put_stats()
    |> case do
      %{error: nil} = so ->
        {:ok, so}

      %{error: e} = so when not is_nil(e) ->
        {:error, so}
    end
  end

  @spec do_search_without_select(SO.t()) :: SO.t()
  def do_search_without_select(%SO{} = so) do
    so
    |> Map.put(
      :stats,
      %{
        start_monotonic_time: System.monotonic_time(:millisecond),
        total_duration: nil
      }
    )
    |> default_from
    |> parse_querystring()
    |> verify_path_in_schema()
    |> apply_local_timestamp_correction()
    |> apply_wheres()
  end
end
