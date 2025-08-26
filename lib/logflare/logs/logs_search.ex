defmodule Logflare.Logs.Search do
  @moduledoc false

  alias Logflare.Logs.SearchOperation, as: SO
  import Logflare.Logs.SearchOperations
  alias Logflare.Logs.SearchQueries
  alias Logflare.Source
  alias Logflare.Sources
  alias Logflare.BqRepo
  alias Logflare.Utils.Tasks
  alias Logflare.Google.BigQuery.GCPConfig
  import Ecto.Query

  @spec search(Logflare.Logs.SearchOperation.t()) :: {:error, any} | {:ok, %{events: any}}
  def search(%SO{} = so) do
    so = get_and_put_partition_by(so)

    tasks = [
      Tasks.async(fn -> search_events(so) end)
    ]

    tasks_with_results = Task.yield_many(tasks, 30_000)

    results =
      tasks_with_results
      |> Enum.map(fn {task, res} ->
        res || Task.shutdown(task, :brutal_kill)
      end)

    [event_result] = results

    case event_result do
      {:ok, {:ok, events_so}} ->
        {:ok, %{events: events_so}}

      {:ok, {:error, result_so}} ->
        {:error, result_so}

      {:error, result_so} ->
        {:error, result_so}

      nil ->
        {:error, "Search task timeout"}
    end
  end

  def aggs(%SO{} = so) do
    so = get_and_put_partition_by(so)

    tasks = [
      Tasks.async(fn -> search_result_aggregates(so) end)
    ]

    tasks_with_results = Task.yield_many(tasks, 30_000)

    results =
      tasks_with_results
      |> Enum.map(fn {task, res} ->
        res || Task.shutdown(task, :brutal_kill)
      end)

    [agg_result] = results

    case agg_result do
      {:ok, {:ok, agg_so}} ->
        {:ok, %{aggregates: agg_so}}

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

    with %{error: nil} = so <- put_chart_data_shape_id(so),
         %{error: nil} = so <- apply_query_defaults(so),
         %{error: nil} = so <- apply_halt_conditions(so),
         %{error: nil} = so <- apply_local_timestamp_correction(so),
         %{error: nil} = so <- apply_timestamp_filter_rules(so),
         %{error: nil} = so <- apply_filters(so),
         %{error: nil} = so <- unnest_log_level(so),
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

  @doc """
  Search for events before and after a selected event.
  """
  def search_event_context(%SO{} = so, log_event_id, %DateTime{} = timestamp) do
    so =
      so
      |> Logflare.Logs.Search.get_and_put_partition_by()

    %{values: [min, max]} =
      so.lql_rules
      |> Logflare.Lql.Rules.get_timestamp_filters()
      |> Enum.find(fn rule -> rule.operator == :range end)

    fields = [:id, :timestamp, :event_message, :metadata]

    before_query =
      from(so.source.bq_table_id)
      |> Logflare.Logs.LogEvents.partition_query([min, max], so.partition_by)
      |> where(
        [t],
        t.timestamp < ^timestamp or (t.timestamp == ^timestamp and t.id <= ^log_event_id)
      )
      |> order_by([t], desc: t.timestamp, desc: t.id)
      |> select([t], map(t, ^fields))
      |> select_merge([t], %{
        rank:
          fragment("1 - ROW_NUMBER() OVER (ORDER BY ? DESC, ? DESC)", t.timestamp, t.id)
          |> selected_as(:rank)
      })
      |> Logflare.Lql.apply_filter_rules(so.lql_rules)
      |> limit(51)
      |> subquery()
      |> select([t], t)

    after_query =
      from(so.source.bq_table_id)
      |> Logflare.Logs.LogEvents.partition_query([min, max], so.partition_by)
      |> where(
        [t],
        t.timestamp > ^timestamp or (t.timestamp == ^timestamp and t.id > ^log_event_id)
      )
      |> order_by([t], asc: t.timestamp, asc: t.id)
      |> select([t], map(t, ^fields))
      |> select_merge([t], %{
        rank:
          over(fragment("ROW_NUMBER()"), order_by: [asc: t.timestamp, asc: t.id])
          |> selected_as(:rank)
      })
      |> Logflare.Lql.apply_filter_rules(so.lql_rules)
      |> limit(50)
      |> subquery()
      |> select([t], t)

    query =
      before_query
      |> union_all(^after_query)
      |> subquery()
      |> select([t], map(t, ^fields))
      |> select_merge([t], %{rank: t.rank})
      |> order_by([t], asc: t.timestamp, asc: t.id)

    %{so | query: query} |> do_query()
  end

  def query_source_streaming_buffer(%Source{} = source) do
    q =
      case Sources.get_table_partition_type(source) do
        :pseudo ->
          SearchQueries.source_table_streaming_buffer(source.bq_table_id)

        :timestamp ->
          SearchQueries.source_table_last_1_minutes(source.bq_table_id)
      end
      |> order_by(desc: :timestamp)
      |> limit(100)

    bq_project_id = source.user.bigquery_project_id || GCPConfig.default_project_id()
    BqRepo.query(source.user, bq_project_id, q)
  end

  def get_and_put_partition_by(%SO{} = so) do
    %{so | partition_by: Sources.get_table_partition_type(so.source)}
  end
end
