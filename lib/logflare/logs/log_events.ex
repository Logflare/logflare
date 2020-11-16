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
    source = Sources.Cache.get_by_id_and_preload(source_token)
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
    source = Sources.Cache.get_by_id_and_preload(source_token)
    bq_table_id = source.bq_table_id
    bq_project_id = source.user.bigquery_project_id || GCPConfig.default_project_id()
    %{bigquery_dataset_id: dataset_id} = GenUtils.get_bq_user_info(source.token)

    base_query = SearchQueries.source_log_event_id(bq_table_id, id)

    fetch_streaming_buffer(bq_project_id, base_query, dataset_id) ||
      fetch_with_partitions_range(bq_project_id, base_query, dataset_id, partitions_range) ||
      {:error, :not_found}
  end

  @spec fetch_event_by_path(atom(), binary(), term()) :: {:ok, map() | nil} | {:error, any()}
  def fetch_event_by_path(source_token, path, value)
      when is_atom(source_token) and is_binary(path) do
    source = Sources.Cache.get_by_id_and_preload(source_token)
    bq_table_id = source.bq_table_id
    bq_project_id = source.user.bigquery_project_id || GCPConfig.default_project_id()
    %{bigquery_dataset_id: dataset_id} = GenUtils.get_bq_user_info(source.token)

    base_query = SearchQueries.source_log_event_by_path(bq_table_id, path, value)

    params = [bq_project_id, base_query, dataset_id]
    apply(&fetch_streaming_buffer/3, params) || apply(&fetch_last_3d/3, params)
  end

  def fetch_streaming_buffer(bq_project_id, query, dataset_id) do
    bq_project_id
    |> BqRepo.query(SearchQueries.where_streaming_buffer(query), dataset_id: dataset_id)
    |> process()
  end

  def fetch_last_3d(bq_project_id, query, dataset_id) do
    bq_project_id
    |> BqRepo.query(where_last_3d_q(query), dataset_id: dataset_id)
    |> process()
  end

  def fetch_with_partitions_range(bq_project_id, query, dataset_id, []) do
    bq_project_id
    |> BqRepo.query(query, dataset_id: dataset_id)
    |> process()
  end

  def fetch_with_partitions_range(bq_project_id, query, dataset_id, [min, max]) do
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
  def where_last_3d_q(q) do
    from_utc = Timex.shift(Timex.today(), days: -3)
    SearchQueries.where_partitiondate_between(q, from_utc, Timex.today())
  end
end
