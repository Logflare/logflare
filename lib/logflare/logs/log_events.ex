defmodule Logflare.Logs.LogEvents do
  @moduledoc false

  alias Logflare.BqRepo
  alias Logflare.Google.BigQuery.GCPConfig
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Lql
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.SourceSchemas
  alias Logflare.Sources

  import Ecto.Query

  @spec fetch_event_by_id(atom(), binary(), Keyword.t()) :: map() | {:error, any()}
  def fetch_event_by_id(source_token, id, opts)
      when is_list(opts) and is_atom(source_token) and is_binary(id) do
    [min, max] = Keyword.get(opts, :partitions_range, [])
    source = Sources.Cache.get_by_and_preload(token: source_token)
    source_schema = SourceSchemas.Cache.get_source_schema_by(source_id: source.id)
    partition_type = Sources.get_table_partition_type(source)

    lql = Keyword.get(opts, :lql, "")
    {:ok, lql_rules} = Lql.decode(lql, source_schema.bigquery_schema)

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

    source.user
    |> BqRepo.query(bq_project_id, query, dataset_id: dataset_id)
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

  def partition_query(query, [min, max], :timestamp) do
    query
    |> where([t], t.timestamp >= ^min)
    |> where([t], t.timestamp <= ^max)
  end

  def partition_query(query, [min, max], :pseudo) do
    {min, Timex.to_date(min)}

    where(
      query,
      [t],
      fragment(
        "_PARTITIONTIME BETWEEN TIMESTAMP_TRUNC(?, DAY) AND TIMESTAMP_TRUNC(?, DAY)",
        ^Timex.to_date(min),
        ^Timex.to_date(max)
      )
    )
  end
end
