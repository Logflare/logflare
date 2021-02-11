defmodule Logflare.Logs.LogEvents do
  @moduledoc false
  use Logflare.Commons
  alias Logflare.Google.BigQuery.GCPConfig
  alias Logflare.Logs.SearchOperations
  alias Logflare.Logs.SearchQueries
  alias Logflare.Google.BigQuery.GenUtils
  import Ecto.Query

  @spec create_log_event(LE.t()) :: {:ok, LE.t()} | {:error, term}
  def create_log_event(%LE{} = le) do
    MemoryRepo.insert(le)
  end

  def get_log_event_by_metadata_for_source(metadata_fragment, source_id)
      when is_integer(source_id) and is_map(metadata_fragment) do
    LE
    |> from()
    |> join(:inner, [le], s in assoc(le, :source))
    |> where([le, s], s.id == ^source_id)
    |> MemoryRepo.all()
    |> Enum.find(&MapSet.subset?(MapSet.new(metadata_fragment), MapSet.new(&1.body.metadata)))
  end

  def get_log_event(id) do
    RepoWithCache.get(LogEvent, id)
  end

  def get_log_event!(id) do
    RepoWithCache.get!(LogEvent, id)
  end

  def get_log_event_with_source_and_partitions(id,
        source: source,
        partitions_range: partitions_range
      ) do
    if le = RepoWithCache.get(LogEvent, id) do
      le
    else
      fetch_event_by_id(source.token, id, partitions_range: partitions_range)
    end
  end

  @spec fetch_event_by_id_and_timestamp(atom, keyword) :: {:ok, map()} | {:error, map()}
  def fetch_event_by_id_and_timestamp(source_token, kw) when is_atom(source_token) do
    id = kw[:id]
    timestamp = kw[:timestamp]
    source = Sources.get_by_id_and_preload(source_token)
    bq_table_id = source.bq_table_id
    query = SearchQueries.source_log_event_query(bq_table_id, id, timestamp)

    %{sql_with_params: sql_with_params, params: params} =
      SearchOperations.Helpers.ecto_query_to_sql(query, source)

    bq_project_id = source.user.bigquery_project_id || GCPConfig.default_project_id()

    query_result = BqRepo.query_with_sql_and_params(bq_project_id, sql_with_params, params)

    with {:ok, result} <- query_result do
      case result do
        %{rows: []} ->
          {:error, :not_found}

        %{rows: [row]} ->
          le = LogEvent.make_from_db(row, %{source: source})
          {:ok, le}

        %{rows: rows} when length(rows) >= 1 ->
          row = Enum.find(rows, &(&1.id == id))
          le = LogEvent.make_from_db(row, %{source: source})
          {:ok, le}
      end
    else
      errtup -> errtup
    end
  end

  @spec fetch_event_by_id(atom(), binary(), Keyword.t()) :: map() | {:error, any()}
  def fetch_event_by_id(source_token, id, opts)
      when is_list(opts) and is_atom(source_token) and is_binary(id) do
    partitions_range = Keyword.get(opts, :partitions_range, [])
    source = Sources.get_by_id_and_preload(source_token)
    bq_table_id = source.bq_table_id
    bq_project_id = source.user.bigquery_project_id || GCPConfig.default_project_id()
    %{bigquery_dataset_id: dataset_id} = GenUtils.get_bq_user_info(source.token)

    base_query = SearchQueries.source_log_event_id(bq_table_id, id)
    partition_type = Sources.get_table_partition_type(source)

    fetch_streaming_buffer(
      bq_project_id,
      base_query,
      dataset_id,
      partition_type
    ) ||
      fetch_with_partitions_range(
        bq_project_id,
        base_query,
        dataset_id,
        partitions_range,
        partition_type
      ) ||
      {:error, :not_found}
  end

  @spec fetch_event_by_path(atom(), binary(), term()) :: {:ok, map() | nil} | {:error, any()}
  def fetch_event_by_path(source_token, path, value)
      when is_atom(source_token) and is_binary(path) do
    source = Sources.get_by_id_and_preload(source_token)
    bq_table_id = source.bq_table_id
    bq_project_id = source.user.bigquery_project_id || GCPConfig.default_project_id()
    %{bigquery_dataset_id: dataset_id} = GenUtils.get_bq_user_info(source.token)

    base_query = SearchQueries.source_log_event_by_path(bq_table_id, path, value)
    partition_type = Sources.get_table_partition_type(source)

    params = [bq_project_id, base_query, dataset_id, partition_type]

    apply(&fetch_streaming_buffer/4, params) || apply(&fetch_last_3d/4, params) ||
      {:error, :not_found}
  end

  defp fetch_streaming_buffer(_, _, _, :timestamp) do
    nil
  end

  defp fetch_streaming_buffer(bq_project_id, query, dataset_id, :pseudo) do
    bq_project_id
    |> BqRepo.query(SearchQueries.where_streaming_buffer(query), dataset_id: dataset_id)
    |> process()
  end

  defp fetch_last_3d(bq_project_id, query, dataset_id, :timestamp) do
    import Ecto.Query
    from_utc = Timex.shift(Timex.today(), days: -3)

    query =
      query
      |> where([t], t.timestamp >= ^from_utc)
      |> where([t], t.timestamp <= ^Date.utc_today())

    bq_project_id
    |> BqRepo.query(query, dataset_id: dataset_id)
    |> process()
  end

  defp fetch_last_3d(bq_project_id, query, dataset_id, :pseudo) do
    bq_project_id
    |> BqRepo.query(where_last_3d_q(query), dataset_id: dataset_id)
    |> process()
  end

  defp fetch_with_partitions_range(bq_project_id, query, dataset_id, [], _) do
    bq_project_id
    |> BqRepo.query(query, dataset_id: dataset_id)
    |> process()
  end

  defp fetch_with_partitions_range(bq_project_id, query, dataset_id, [min, max], :timestamp) do
    import Ecto.Query

    query =
      query
      |> where([t], t.timestamp >= ^min)
      |> where([t], t.timestamp <= ^max)

    bq_project_id
    |> BqRepo.query(query, dataset_id: dataset_id)
    |> process()
  end

  defp fetch_with_partitions_range(bq_project_id, query, dataset_id, [min, max], :pseudo) do
    query = SearchQueries.where_partitiondate_between(query, min, max)

    bq_project_id
    |> BqRepo.query(query, dataset_id: dataset_id)
    |> process()
  end

  defp process(result) do
    case result do
      {:ok, %{rows: rows}} when length(rows) > 1 ->
        {:error, "Multiple rows returned, expected one"}

      {:ok, %{rows: [row]}} ->
        row

      {:ok, %{rows: []}} ->
        nil

      {:error, error} ->
        {:error, error}
    end
  end

  @spec where_last_3d_q(any) :: Ecto.Query.t()
  defp where_last_3d_q(q) do
    from_utc = Timex.shift(Timex.today(), days: -3)
    SearchQueries.where_partitiondate_between(q, from_utc, Timex.today())
  end

  def get_log_event_by(kw) do
    RepoWithCache.get_by(LogEvent, kw)
  end
end
