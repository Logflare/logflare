defmodule Logflare.Logs.LogEvents do
  @moduledoc false
  alias Logflare.Google.BigQuery.GCPConfig
  alias Logflare.Sources
  alias Logflare.LogEvent
  alias Logflare.Logs.SearchOperations
  alias Logflare.Logs.SearchQueries
  alias Logflare.BqRepo
  alias Logflare.Google.BigQuery.GenUtils
  # import Logflare.Ecto.BQQueryAPI

  @spec fetch_event_by_id_and_timestamp(atom, keyword) :: {:ok, map()} | {:error, map()}
  def fetch_event_by_id_and_timestamp(source_token, kw) when is_atom(source_token) do
    id = kw[:id]
    timestamp = kw[:timestamp]
    source = Sources.get_by_and_preload(token: source_token)
    bq_table_id = source.bq_table_id
    query = SearchQueries.source_log_event_query(bq_table_id, id, timestamp)

    %{sql_with_params: sql_with_params, params: params} =
      SearchOperations.Helpers.ecto_query_to_sql(query, source)

    bq_project_id = source.user.bigquery_project_id || GCPConfig.default_project_id()

    query_result =
      BqRepo.query_with_sql_and_params(source.user, bq_project_id, sql_with_params, params)

    with {:ok, result} <- query_result do
      case result do
        %{rows: []} ->
          {:error, :not_found}

        %{rows: [row]} ->
          le = LogEvent.make_from_db(row, %{source: source})
          {:ok, le}

        %{rows: rows} ->
          row = Enum.find(rows, &(&1.id == id))
          le = LogEvent.make_from_db(row, %{source: source})
          {:ok, le}
      end
    end
  end

  @spec fetch_event_by_id(atom(), binary(), Keyword.t()) :: map() | {:error, any()}
  def fetch_event_by_id(source_token, id, opts)
      when is_list(opts) and is_atom(source_token) and is_binary(id) do
    partitions_range = Keyword.get(opts, :partitions_range, [])
    source = Sources.Cache.get_by_and_preload(token: source_token)
    bq_table_id = source.bq_table_id
    bq_project_id = source.user.bigquery_project_id || GCPConfig.default_project_id()
    %{bigquery_dataset_id: dataset_id} = GenUtils.get_bq_user_info(source.token)

    base_query = SearchQueries.source_log_event_id(bq_table_id, id)
    partition_type = Sources.get_table_partition_type(source)

    fetch_streaming_buffer(
      source.user,
      bq_project_id,
      base_query,
      dataset_id,
      partition_type
    ) ||
      fetch_with_partitions_range(
        source.user,
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
    source = Sources.get_by_and_preload(token: source_token)
    bq_table_id = source.bq_table_id
    bq_project_id = source.user.bigquery_project_id || GCPConfig.default_project_id()
    %{bigquery_dataset_id: dataset_id} = GenUtils.get_bq_user_info(source.token)

    base_query = SearchQueries.source_log_event_by_path(bq_table_id, path, value)
    partition_type = Sources.get_table_partition_type(source)

    params = [source.user, bq_project_id, base_query, dataset_id, partition_type]

    apply(&fetch_streaming_buffer/5, params) || apply(&fetch_last_3d/5, params) ||
      {:error, :not_found}
  end

  defp fetch_streaming_buffer(_, _, _, _, :timestamp) do
    nil
  end

  defp fetch_streaming_buffer(user, bq_project_id, query, dataset_id, :pseudo) do
    user
    |> BqRepo.query(bq_project_id, SearchQueries.where_streaming_buffer(query),
      dataset_id: dataset_id
    )
    |> process()
  end

  defp fetch_last_3d(user, bq_project_id, query, dataset_id, :timestamp) do
    import Ecto.Query
    from_utc = Timex.shift(Timex.today(), days: -3)

    query =
      query
      |> where([t], t.timestamp >= ^from_utc)
      |> where([t], t.timestamp <= ^Date.utc_today())

    user
    |> BqRepo.query(bq_project_id, query, dataset_id: dataset_id)
    |> process()
  end

  defp fetch_last_3d(user, bq_project_id, query, dataset_id, :pseudo) do
    user
    |> BqRepo.query(bq_project_id, where_last_3d_q(query), dataset_id: dataset_id)
    |> process()
  end

  defp fetch_with_partitions_range(user, bq_project_id, query, dataset_id, [], _) do
    user
    |> BqRepo.query(bq_project_id, query, dataset_id: dataset_id)
    |> process()
  end

  defp fetch_with_partitions_range(user, bq_project_id, query, dataset_id, [min, max], :timestamp) do
    import Ecto.Query

    query =
      query
      |> where([t], t.timestamp >= ^min)
      |> where([t], t.timestamp <= ^max)

    user
    |> BqRepo.query(bq_project_id, query, dataset_id: dataset_id)
    |> process()
  end

  defp fetch_with_partitions_range(user, bq_project_id, query, dataset_id, [min, max], :pseudo) do
    query = SearchQueries.where_partitiondate_between(query, min, max)

    user
    |> BqRepo.query(bq_project_id, query, dataset_id: dataset_id)
    |> process()
  end

  defp process(result) do
    case result do
      {:ok, %{rows: []}} ->
        nil

      {:ok, %{rows: [row]}} ->
        row

      {:ok, %{rows: _rows}} ->
        {:error, "Multiple rows returned, expected one"}

      {:error, error} ->
        {:error, error}
    end
  end

  @spec where_last_3d_q(any) :: Ecto.Query.t()
  defp where_last_3d_q(q) do
    from_utc = Timex.shift(Timex.today(), days: -3)
    SearchQueries.where_partitiondate_between(q, from_utc, Timex.today())
  end
end
