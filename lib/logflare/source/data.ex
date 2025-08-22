defmodule Logflare.Source.Data do
  @moduledoc false
  alias Logflare.Sources.Counters
  alias Logflare.Google.BigQuery
  alias Logflare.Source.RateCounterServer
  alias Logflare.SourceSchemas
  alias Logflare.Source.BigQuery.SchemaBuilder
  alias Logflare.Google.BigQuery.SchemaUtils

  @spec get_log_count(atom, String.t()) :: non_neg_integer()
  def get_log_count(token, _bigquery_project_id) do
    case BigQuery.get_table(token) do
      {:ok, table_info} ->
        table_rows = String.to_integer(table_info.numRows)
        buffer_rows = get_streaming_buffer_rows(table_info.streamingBuffer)

        table_rows + buffer_rows

      {:error, _message} ->
        0
    end
  end

  defp get_streaming_buffer_rows(nil), do: 0
  defp get_streaming_buffer_rows(%{estimatedRows: nil}), do: 0
  defp get_streaming_buffer_rows(%{estimatedRows: rows}), do: String.to_integer(rows)

  @spec get_total_inserts(atom) :: non_neg_integer
  def get_total_inserts(source_id) when is_atom(source_id) do
    get_node_inserts(source_id) + get_bq_inserts(source_id)
  end

  def get_node_inserts(source_id) do
    {:ok, inserts} = Counters.get_inserts(source_id)
    inserts
  end

  def get_bq_inserts(source_id) do
    {:ok, bq_inserts} = Counters.get_bq_inserts(source_id)
    bq_inserts
  end

  @spec get_rate(map) :: non_neg_integer
  def get_rate(%{id: _id, name: _name, token: source_token}) do
    {:ok, source_id} = Ecto.UUID.Atom.load(source_token)

    RateCounterServer.get_rate(source_id)
  end

  @spec get_rate(atom) :: non_neg_integer
  def get_rate(source_id) when is_atom(source_id) do
    RateCounterServer.get_rate(source_id)
  end

  @spec get_avg_rate(map) :: non_neg_integer
  def get_avg_rate(%{id: _id, name: _name, token: source_token}) do
    {:ok, source_id} = Ecto.UUID.Atom.load(source_token)

    RateCounterServer.get_avg_rate(source_id)
  end

  @spec get_avg_rate(atom) :: non_neg_integer
  def get_avg_rate(source_id) when is_atom(source_id) do
    RateCounterServer.get_avg_rate(source_id)
  end

  @spec get_max_rate(atom) :: integer
  def get_max_rate(source_id) do
    RateCounterServer.get_max_rate(source_id)
  end

  @spec get_schema_field_count(struct()) :: non_neg_integer
  def get_schema_field_count(source) do
    source_schema = SourceSchemas.Cache.get_source_schema_by(source_id: source.id)

    if source_schema do
      source_schema.bigquery_schema
      |> SchemaUtils.bq_schema_to_flat_typemap()
      |> Enum.count()
    else
      SchemaBuilder.initial_table_schema()
      |> SchemaUtils.bq_schema_to_flat_typemap()
      |> Enum.count()
    end
  end
end
