defmodule Logflare.Source.Data do
  @moduledoc false
  alias Logflare.Sources.Counters
  alias Logflare.Google.BigQuery
  alias Logflare.Source.{RateCounterServer, BigQuery.Buffer, BigQuery.Schema}

  def get_logs(source_id) when is_atom(source_id) do
    Logflare.Source.RecentLogsServer.list(source_id)
  end

  @spec get_log_count(atom, String.t()) :: non_neg_integer()
  def get_log_count(token, _bigquery_project_id) do
    case BigQuery.get_table(token) do
      {:ok, table_info} ->
        table_rows = String.to_integer(table_info.numRows)

        buffer_rows =
          case is_nil(table_info.streamingBuffer) do
            true ->
              0

            false ->
              case is_nil(table_info.streamingBuffer.estimatedRows) do
                true ->
                  0

                false ->
                  String.to_integer(table_info.streamingBuffer.estimatedRows)
              end
          end

        table_rows + buffer_rows

      {:error, _message} ->
        0
    end
  end

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

  @spec get_buffer(atom, integer) :: integer
  def get_buffer(source_id, fallback \\ 0) do
    case Process.whereis(String.to_atom("#{source_id}-buffer")) do
      nil ->
        fallback

      _ ->
        Buffer.get_count(source_id)
    end
  end

  @spec get_schema_field_count(struct()) :: non_neg_integer
  def get_schema_field_count(source) do
    case Process.whereis(String.to_atom("#{source.token}-schema")) do
      nil ->
        0

      _ ->
        Schema.get_state(source.token).field_count
    end
  end
end
