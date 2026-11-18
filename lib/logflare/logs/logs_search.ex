defmodule Logflare.Logs.Search do
  @moduledoc false

  alias Logflare.Logs.EventPage
  alias Logflare.Logs.LogEvents
  alias Logflare.Logs.SearchOperation, as: SO
  alias Logflare.Lql.Rules, as: LqlRules
  alias Logflare.Sources
  import Ecto.Query
  import Logflare.Logs.SearchOperations

  @context_page_size 50

  @type context_page ::
          Range.t() | %{intent: EventPage.direction(), cursor: EventPage.cursor()}

  @spec context_page_size() :: pos_integer()
  def context_page_size, do: @context_page_size

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
  @spec search_event_context(SO.t(), String.t(), DateTime.t(), context_page()) ::
          {:ok, SO.t()} | {:error, SO.t()}
  def search_event_context(
        %SO{} = so,
        log_event_id,
        %DateTime{} = timestamp,
        page \\ -@context_page_size..@context_page_size
      ) do
    so =
      %{so | type: :events}
      |> get_and_put_partition_by()

    %{values: [min, max]} =
      so.lql_rules
      |> LqlRules.get_timestamp_filters()
      |> Enum.find(fn rule -> rule.operator == :range end)

    query =
      so
      |> table_name()
      |> from()
      |> where([t], t.timestamp >= ^min and t.timestamp <= ^max)
      |> select([t], map(t, [:id, :timestamp, :event_message]))
      |> partition_query(so, [min, max])

    query =
      case so.backend_type do
        :postgres ->
          select_merge(query, [t], %{metadata: fragment("?->'metadata'", t.body)})

        :bigquery ->
          select_merge(query, [t], %{metadata: t.metadata})
      end

    query =
      query
      |> Logflare.Lql.apply_filter_rules(so.lql_meta_and_msg_filters, dialect: so.backend_type)
      |> context_query(so, log_event_id, timestamp, page)

    with so <- %{so | query: query},
         %{error: nil} = so <- do_query(so) do
      {:ok, so}
    else
      so -> {:error, so}
    end
  end

  @spec partition_query(Ecto.Query.t(), SO.t(), [DateTime.t()]) :: Ecto.Query.t()
  defp partition_query(query, %SO{backend_type: :bigquery, partition_by: :pseudo}, range) do
    LogEvents.partition_query(query, range, :pseudo)
  end

  defp partition_query(query, _so, _range), do: query

  @spec context_query(
          Ecto.Query.t(),
          SO.t(),
          String.t(),
          DateTime.t(),
          context_page()
        ) ::
          Ecto.Query.t()
  defp context_query(
         query,
         so,
         _id,
         _timestamp,
         %{intent: intent, cursor: %{timestamp: _, id: _}} = request
       )
       when intent in [:previous, :next] do
    %{query: query} = apply_cursor(%{so | query: query, event_page_request: request})

    query =
      case intent do
        :previous -> order_by(query, [t], desc: t.timestamp, desc: t.id)
        :next -> order_by(query, [t], asc: t.timestamp, asc: t.id)
      end

    query
    |> limit(^(@context_page_size + 1))
    |> subquery()
    |> order_by([t], asc: t.timestamp, asc: t.id)
    |> select([t], t)
  end

  defp context_query(query, _so, log_event_id, timestamp, %Range{} = ranks) do
    numbered_rows =
      query
      |> select_merge([t], %{
        row_num: fragment("ROW_NUMBER() OVER (ORDER BY ? ASC, ? ASC)", t.timestamp, t.id)
      })

    target_position =
      from(nr in subquery(numbered_rows))
      |> where([nr], nr.id == ^log_event_id and nr.timestamp == ^timestamp)
      |> select([nr], %{target_row_num: nr.row_num})

    from(subquery(numbered_rows))
    |> join(:cross, subquery(target_position))
    |> where(
      [nr, tp],
      nr.row_num >= tp.target_row_num + ^ranks.first and
        nr.row_num <= tp.target_row_num + ^ranks.last
    )
    |> order_by([nr, _], asc: nr.timestamp, asc: nr.id)
    |> select([nr, tp], %{
      id: nr.id,
      timestamp: nr.timestamp,
      event_message: nr.event_message,
      metadata: nr.metadata,
      rank: fragment("? - ?", nr.row_num, tp.target_row_num) |> selected_as(:rank)
    })
  end

  def get_and_put_partition_by(%SO{} = so) do
    %{so | partition_by: Sources.get_table_partition_type(so.source)}
  end
end
