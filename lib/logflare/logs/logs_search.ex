defmodule Logflare.Logs.Search do
  @moduledoc false

  import Ecto.Query
  import Logflare.Logs.SearchOperations

  alias Logflare.Logs.LogEvents
  alias Logflare.Logs.SearchOperation, as: SO
  alias Logflare.Logs.SearchOperations.Helpers, as: SearchOperationHelpers
  alias Logflare.Lql.Rules, as: LqlRules
  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Sources

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

  @spec aggs(SO.t()) :: {:ok, %{aggregates: SO.t()}} | {:error, SO.t()}
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
         %{error: nil} = so <- apply_filters(so),
         %{error: nil} = so <- apply_cursor(so),
         %{error: nil} = so <- apply_select_rules(so),
         %{error: nil} = so <- do_query(so),
         %{error: nil} = so <- apply_warning_conditions(so),
         %{error: nil} = so <- put_stats(so) do
      {:ok, so}
    else
      so -> {:error, so}
    end
  end

  @spec search_result_aggregates(SO.t()) :: {:ok, SO.t()} | {:error, SO.t()}
  def search_result_aggregates(%SO{} = so) do
    so = %{so | type: :aggregates} |> put_time_stats()

    with %{error: nil} = so <- apply_halt_conditions(so),
         %{error: nil} = so <- put_chart_data_shape_id(so),
         %{error: nil} = so <- apply_local_timestamp_correction(so),
         %{error: nil} = so <- apply_timestamp_filter_rules(so),
         %{error: nil} = so <- prepare_aggregate_query(so),
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

  @spec prepare_aggregate_query(SO.t()) :: SO.t()
  defp prepare_aggregate_query(%SO{chart_rules: [_, _ | _] = chart_rules} = so) do
    series =
      chart_rules
      |> Enum.with_index()
      |> Enum.map(fn {%ChartRule{path: path, aggregate: aggregate}, index} ->
        field =
          if path == "timestamp", do: "*", else: String.replace_leading(path, "metadata.", "m.")

        %{key: "aggregate_#{index}", label: "#{aggregate}(#{field})"}
      end)

    prepared_aggregates =
      chart_rules
      |> Enum.zip(series)
      |> Enum.group_by(fn {rule, _series} -> {rule.path, rule.period, rule.value_type} end)
      |> Enum.map(fn {_group_key, group} ->
        {rules, series} = Enum.unzip(group)

        apply_numeric_aggs(
          %{so | chart_rules: rules, series: series},
          Enum.map(rules, & &1.aggregate)
        )
      end)

    %{
      so
      | query: combined_aggregate_query(so, prepared_aggregates),
        series: series
    }
  end

  defp prepare_aggregate_query(%SO{} = so), do: apply_numeric_aggs(so)

  @spec combined_aggregate_query(SO.t(), [SO.t()]) :: Ecto.Query.t()
  defp combined_aggregate_query(so, prepared_aggregates) do
    %{chart_rules: [%{period: period} | _]} = so

    %{min: min, max: max} =
      SearchOperationHelpers.get_min_max_filter_timestamps(so.lql_ts_filters, period)

    bounds = SearchOperationHelpers.aggregate_bucket_bounds(min, max, period)
    seconds = %{second: 1, minute: 60, hour: 3_600, day: 86_400}[period]

    timestamps =
      aggregate_timestamp_padding(
        so.backend_type,
        Timex.to_naive_datetime(bounds.min),
        Timex.to_naive_datetime(bounds.max),
        seconds
      )

    timestamps = if min == max, do: where(timestamps, false), else: timestamps

    query =
      from(t in "chart_timestamps", select: %{})
      |> with_cte("chart_timestamps", as: ^timestamps)

    {query, timestamp} =
      prepared_aggregates
      |> Enum.with_index(1)
      |> Enum.reduce({query, dynamic([t], t.timestamp)}, fn {aggregate, index},
                                                            {query, timestamp} ->
        key = hd(aggregate.series).key
        on = dynamic([{a, index}], ^timestamp == a.timestamp)

        query =
          query
          |> with_cte(^key, as: ^aggregate.query)
          |> join(:full, [], a in ^key, on: ^on)

        query =
          aggregate.chart_rules
          |> Enum.zip(aggregate.series)
          |> Enum.reduce(query, fn {%ChartRule{aggregate: field}, %{key: key}}, query ->
            select_merge(query, [..., a], %{
              ^key =>
                fragment(
                  "CASE WHEN ? IS NULL THEN 0 ELSE ? END AS ?",
                  a.timestamp,
                  field(a, ^field),
                  identifier(^key)
                )
            })
          end)

        {query, dynamic([{a, index}], coalesce(^timestamp, a.timestamp))}
      end)

    selected_timestamp = dynamic(selected_as(^timestamp, :timestamp))

    query
    |> select_merge(^%{timestamp: selected_timestamp})
    |> order_by(^[desc: timestamp])
  end

  @spec aggregate_timestamp_padding(
          :bigquery | :postgres,
          NaiveDateTime.t(),
          NaiveDateTime.t(),
          pos_integer()
        ) :: Ecto.Query.t()
  defp aggregate_timestamp_padding(:bigquery, min, max, seconds) do
    min = min |> DateTime.from_naive!("Etc/UTC") |> DateTime.to_unix(:microsecond)
    max = max |> DateTime.from_naive!("Etc/UTC") |> DateTime.to_unix(:microsecond)

    from(
      t in fragment(
        "(SELECT timestamp FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(TIMESTAMP_MICROS(?), TIMESTAMP_MICROS(?), INTERVAL ? SECOND)) AS timestamp)",
        ^min,
        ^max,
        ^seconds
      ),
      select: %{timestamp: t.timestamp}
    )
  end

  defp aggregate_timestamp_padding(:postgres, min, max, seconds) do
    from(
      t in fragment(
        "(SELECT generate_series(?::timestamp, ?::timestamp, ? * INTERVAL '1 second') AS timestamp)",
        ^min,
        ^max,
        ^seconds
      ),
      select: %{timestamp: t.timestamp}
    )
  end

  @doc """
  Search for events before and after a selected event.
  """
  @spec search_event_context(SO.t(), String.t(), DateTime.t()) :: {:ok, SO.t()} | {:error, SO.t()}
  def search_event_context(%SO{} = so, log_event_id, %DateTime{} = timestamp) do
    so =
      so
      |> get_and_put_partition_by()

    %{values: [min, max]} =
      so.lql_rules
      |> LqlRules.get_timestamp_filters()
      |> Enum.find(fn rule -> rule.operator == :range end)

    fields = [:id, :timestamp, :event_message, :metadata]

    numbered_rows =
      from(so.source.bq_table_id)
      |> LogEvents.partition_query([min, max], so.partition_by)
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

  def get_and_put_partition_by(%SO{} = so) do
    %{so | partition_by: Sources.get_table_partition_type(so.source)}
  end
end
