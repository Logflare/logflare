defmodule Logflare.Logs.LogEvents do
  @moduledoc false

  require Logflare.Ecto.BQQueryAPI

  import Ecto.Query
  import Logflare.Ecto.BQQueryAPI, only: [in_streaming_buffer: 0]
  import Logflare.Utils.Guards

  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Billing
  alias Logflare.Google.BigQuery.GCPConfig
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.LogEvent, as: LE
  alias Logflare.Logs.LogEvents
  alias Logflare.Lql
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.SourceSchemas
  alias Logflare.Sources
  alias Logflare.Sources.Source.BigQuery.SchemaBuilder

  @doc """
  Fetches a log event from BigQuery by ID with partition and LQL filtering.
  """
  @spec fetch_event_by_id(source_token :: atom(), id :: binary(), opts :: Keyword.t()) ::
          map() | {:error, any()}
  def fetch_event_by_id(source_token, id, opts)
      when is_atom_value(source_token) and is_non_empty_binary(id) and is_list(opts) do
    [min, max] = Keyword.get(opts, :partitions_range, [])
    source = Sources.Cache.get_by_and_preload(token: source_token)
    source_schema = SourceSchemas.Cache.get_source_schema_by(source_id: source.id)
    partition_type = Sources.get_table_partition_type(source)

    lql = Keyword.get(opts, :lql, "")

    {:ok, lql_rules} =
      Lql.decode(
        lql,
        Map.get(source_schema || %{}, :bigquery_schema, SchemaBuilder.initial_table_schema())
      )

    lql_rules =
      lql_rules
      |> Enum.filter(fn
        %FilterRule{path: "timestamp"} -> false
        %FilterRule{} -> true
        _ -> false
      end)

    bq_table_id = source.bq_table_id
    bq_project_id = source.user.bigquery_project_id || GCPConfig.default_project_id()
    %{bigquery_dataset_id: dataset_id} = GenUtils.get_bq_user_info(source.token)

    query =
      from(bq_table_id)
      |> Lql.apply_filter_rules(lql_rules)
      |> where([t], t.id == ^id)
      |> partition_query([min, max], partition_type)
      |> select([t], fragment("*"))

    BigQueryAdaptor.execute_query({bq_project_id, dataset_id, source.user.id}, query,
      query_type: :search
    )
    |> case do
      {:ok, %{rows: []}} ->
        {:error, :not_found}

      {:ok, %{rows: [row]}} ->
        row

      {:ok, %{rows: _rows}} ->
        {:error, "Multiple rows returned, expected one"}

      {:error, error} ->
        {:error, error}
    end
  end

  @doc """
  Retrieves a log event by ID from cache with fallback to BigQuery.

  ## Options

    * `:timestamp` - DateTime of the log event, used to calculate partition range (±1 hour)
    * `:source` - Source struct, required if timestamp is not provided
    * `:user` - User struct, required if timestamp is not provided (for TTL calculation)
    * `:lql` - LQL query string for additional filtering (optional, defaults to "")
  """
  @spec get_event_with_fallback(source_token :: atom(), log_id :: binary(), opts :: Keyword.t()) ::
          {:ok, LE.t()} | {:error, :not_found | any()}
  def get_event_with_fallback(source_token, log_id, opts)
      when is_atom_value(source_token) and is_non_empty_binary(log_id) and is_list(opts) do
    case LogEvents.Cache.get(source_token, log_id) do
      {:ok, %LE{} = le} ->
        {:ok, le}

      _ ->
        range = calculate_partition_range(opts)

        case LogEvents.Cache.fetch_event_by_id(source_token, log_id,
               partitions_range: range,
               lql: Keyword.get(opts, :lql, "")
             ) do
          %{} = bq_row ->
            source =
              Keyword.get(opts, :source) || Sources.Cache.get_by_and_preload(token: source_token)

            le = LE.make_from_db(bq_row, %{source: source})

            LogEvents.Cache.put(source_token, le.id, le)

            {:ok, le}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  @doc """
  Applies partition filtering to a BigQuery Ecto query based on partition type.
  """
  @spec partition_query(Ecto.Query.t(), [DateTime.t()], :timestamp | :pseudo) :: Ecto.Query.t()
  def partition_query(query, [min, max], :timestamp) do
    query
    |> where([t], t.timestamp >= ^min)
    |> where([t], t.timestamp <= ^max)
  end

  @spec partition_query(Ecto.Query.t(), [DateTime.t()], :timestamp | :pseudo) :: Ecto.Query.t()
  def partition_query(query, [min, max], :pseudo) do
    where(
      query,
      [t],
      fragment(
        "_PARTITIONTIME BETWEEN TIMESTAMP_TRUNC(?, DAY) AND TIMESTAMP_TRUNC(?, DAY)",
        ^Timex.to_date(min),
        ^Timex.to_date(max)
      ) or in_streaming_buffer()
    )
  end

  @spec calculate_partition_range(Keyword.t()) :: [DateTime.t()]
  defp calculate_partition_range(opts) when is_list(opts) do
    cond do
      timestamp = Keyword.get(opts, :timestamp) ->
        calculate_partition_range_from_timestamp(timestamp)

      source = Keyword.get(opts, :source) ->
        user = Keyword.get(opts, :user)
        d = Date.utc_today()
        plan = Billing.Cache.get_plan_by_user(user)
        ttl = Sources.source_ttl_to_days(source, plan)

        [Timex.shift(d, days: -min(ttl, 7)), Timex.shift(d, days: 1)]

      true ->
        raise ArgumentError, "Either :timestamp or both :source and :user must be provided"
    end
  end

  @spec calculate_partition_range_from_timestamp(DateTime.t()) :: [DateTime.t()]
  defp calculate_partition_range_from_timestamp(timestamp) when is_second_precision(timestamp) do
    [Timex.shift(timestamp, hours: -1), Timex.shift(timestamp, hours: 1)]
  end

  defp calculate_partition_range_from_timestamp(%DateTime{}) do
    raise ArgumentError, "timestamp must be second precision"
  end
end
