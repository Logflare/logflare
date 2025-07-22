defmodule Logflare.Logs.LogEvents do
  @moduledoc false
  alias Logflare.Google.BigQuery.GCPConfig
  alias Logflare.Sources
  alias Logflare.SourceSchemas
  alias Logflare.BqRepo
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Lql

  import Ecto.Query

  @spec fetch_event_by_id(atom(), binary(), Keyword.t()) :: map() | {:error, any()}
  def fetch_event_by_id(source_token, id, opts)
      when is_list(opts) and is_atom(source_token) and is_binary(id) do
    [min, max] = Keyword.get(opts, :partitions_range, [])
    source = Sources.Cache.get_by_and_preload(token: source_token)
    source_schema = SourceSchemas.Cache.get_source_schema_by(source_id: source.id)
    lql = Keyword.get(opts, :lql, "")
    {:ok, lql_rules} = Lql.decode(lql, source_schema.bigquery_schema)

    lql_rules =
      lql_rules
      |> Enum.filter(fn
        %Lql.FilterRule{path: "timestamp"} -> false
        %Lql.FilterRule{} -> true
        _ -> false
      end)
      |> dbg()

    bq_table_id = source.bq_table_id
    bq_project_id = source.user.bigquery_project_id || GCPConfig.default_project_id()
    %{bigquery_dataset_id: dataset_id} = GenUtils.get_bq_user_info(source.token)

    query =
      from(bq_table_id)
      |> Lql.EctoHelpers.apply_filter_rules_to_query(lql_rules)
      |> where([t], t.timestamp >= ^min)
      |> where([t], t.timestamp <= ^max)
      |> where([t], t.id == ^id)
      |> select([t], fragment("*"))
      |> dbg()

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
end
