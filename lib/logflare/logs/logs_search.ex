defmodule Logflare.Logs.Search do
  @moduledoc false

  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Google.BigQuery.GCPConfig
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Logs.SearchOperation, as: SO
  alias Logflare.Logs.SearchQueries
  alias Logflare.Sources
  alias Logflare.Sources.Source
  import Ecto.Query
  import Logflare.Logs.SearchOperations

  @spec search(Logflare.Logs.SearchOperation.t()) :: {:error, any} | {:ok, %{events: any}}
  def search(%SO{} = so) do
    so
    |> get_and_put_partition_by()
    |> search_events()
    |> case do
      {:ok, events_so} ->
        {:ok, %{events: events_so}}

      {:error, result_so} ->
        {:error, result_so}
    end
  end

  def aggs(%SO{} = so) do
    so
    |> get_and_put_partition_by()
    |> search_result_aggregates()
    |> case do
      {:ok, agg_so} ->
        {:ok, %{aggregates: agg_so}}

      {:error, result_so} ->
        {:error, result_so}
    end
  end

  def search_events(%SO{} = so) do
    so = %{so | type: :events} |> put_time_stats()

    with %{error: nil} = so <- put_chart_data_shape_id(so),
         %{error: nil} = so <- apply_query_defaults(so),
         %{error: nil} = so <- apply_halt_conditions(so),
         %{error: nil} = so <- apply_local_timestamp_correction(so),
         %{error: nil} = so <- apply_timestamp_filter_rules(so),
         %{error: nil} = so <- apply_select_rules(so),
         %{error: nil} = so <- apply_filters(so),
         %{error: nil} = so <- unnest_log_level(so),
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
  @spec search_event_context(SO.t(), String.t(), DateTime.t()) :: {:ok, SO.t()} | {:error, SO.t()}
  def search_event_context(%SO{} = so, log_event_id, %DateTime{} = timestamp) do
    so =
      so
      |> Logflare.Logs.Search.get_and_put_partition_by()

    %{values: [min, max]} =
      so.lql_rules
      |> Logflare.Lql.Rules.get_timestamp_filters()
      |> Enum.find(fn rule -> rule.operator == :range end)

    fields = [:id, :timestamp, :event_message, :metadata]

    numbered_rows =
      from(so.source.bq_table_id)
      |> Logflare.Logs.LogEvents.partition_query([min, max], so.partition_by)
      |> Logflare.Lql.apply_filter_rules(so.lql_rules)
      |> where([t], t.timestamp >= ^min and t.timestamp <= ^max)
      |> select([t], map(t, ^fields))
      |> select_merge([t], %{
        row_num: fragment("ROW_NUMBER() OVER (ORDER BY ? ASC, ? ASC)", t.timestamp, t.id)
      })

    target_position =
      from(nr in subquery(numbered_rows))
      |> where([nr], nr.id == ^log_event_id and nr.timestamp == ^timestamp)
      |> select([nr], %{target_row_num: nr.row_num})

    query =
      from(subquery(numbered_rows))
      |> join(:cross, subquery(target_position))
      |> where(
        [nr, tp],
        nr.row_num >= tp.target_row_num - 50 and nr.row_num <= tp.target_row_num + 50
      )
      |> order_by([nr, _], asc: nr.timestamp, asc: nr.id)
      |> select([nr, tp], %{
        id: nr.id,
        timestamp: nr.timestamp,
        event_message: nr.event_message,
        metadata: nr.metadata,
        rank: fragment("? - ?", nr.row_num, tp.target_row_num) |> selected_as(:rank)
      })

    with so <- %{so | query: query},
         %{error: nil} = so <- do_query(so) do
      {:ok, so}
    else
      so -> {:error, so}
    end
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
    %{bigquery_dataset_id: dataset_id} = GenUtils.get_bq_user_info(source.token)

    BigQueryAdaptor.execute_query({bq_project_id, dataset_id, source.user.id}, q,
      query_type: :search
    )
  end

  def get_and_put_partition_by(%SO{} = so) do
    %{so | partition_by: Sources.get_table_partition_type(so.source)}
  end
end
