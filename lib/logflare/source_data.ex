defmodule Logflare.SourceData do
  alias Logflare.TableRateCounter
  alias Logflare.TableBuffer
  alias Logflare.TableCounter
  alias Logflare.Google.BigQuery

  @spec get_log_count(:atom, String.t()) :: {}
  def get_log_count(token, bigquery_project_id) do
    case BigQuery.get_table(token, bigquery_project_id) do
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

  def get_ets_count(token) do
    log_table_info = :ets.info(token)

    case log_table_info do
      :undefined ->
        0

      _ ->
        log_table_info[:size]
    end
  end

  def get_total_inserts(token) do
    log_table_info = :ets.info(String.to_atom(token))

    case log_table_info do
      :undefined ->
        0

      _ ->
        {:ok, inserts} = TableCounter.get_total_inserts(String.to_atom(token))
        inserts
    end
  end

  def get_rate(%{id: _id, name: _name, token: source_token}) do
    {:ok, token} = Ecto.UUID.load(source_token)
    website_table = String.to_atom(token)

    get_rate_int(website_table)
  end

  def get_rate(website_table) do
    get_rate_int(website_table)
  end

  def get_logs(table_id) do
    case :ets.info(table_id) do
      :undefined ->
        []

      _ ->
        List.flatten(:ets.match(table_id, {:_, :"$1"}))
    end
  end

  def get_avg_rate(%{id: _id, name: _name, token: source_token}) do
    {:ok, token} = Ecto.UUID.load(source_token)
    website_table = String.to_atom(token)

    case :ets.info(website_table) do
      :undefined ->
        0

      _ ->
        TableRateCounter.get_avg_rate(website_table)
    end
  end

  def get_avg_rate(website_table) do
    case :ets.info(website_table) do
      :undefined ->
        0

      _ ->
        TableRateCounter.get_avg_rate(website_table)
    end
  end

  def get_max_rate(%{id: _id, name: _name, token: source_token}) do
    {:ok, token} = Ecto.UUID.load(source_token)
    website_table = String.to_atom(token)

    case :ets.info(website_table) do
      :undefined ->
        0

      _ ->
        TableRateCounter.get_max_rate(website_table)
    end
  end

  def get_latest_date(source, fallback \\ 0) do
    {:ok, token} = Ecto.UUID.load(source.token)
    website_table = String.to_atom(token)

    case :ets.info(website_table) do
      :undefined ->
        fallback

      _ ->
        case :ets.last(website_table) do
          :"$end_of_table" ->
            fallback

          {timestamp, _unique_int, _monotime} ->
            timestamp
        end
    end
  end

  def get_buffer(token, fallback \\ 0) do
    case Process.whereis(String.to_atom(token <> "-buffer")) do
      nil ->
        fallback

      _ ->
        TableBuffer.get_count(token)
    end
  end

  defp get_rate_int(website_table) do
    case :ets.info(website_table) do
      :undefined ->
        0

      _ ->
        TableRateCounter.get_rate(website_table)
    end
  end
end
