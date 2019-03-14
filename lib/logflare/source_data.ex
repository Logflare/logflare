defmodule Logflare.SourceData do
  def get_log_count(source) do
    log_table_info = :ets.info(String.to_atom(elem(Ecto.UUID.load(source.token), 1)))

    case log_table_info do
      :undefined ->
        0

      _ ->
        log_table_info[:size]
    end
  end

  def get_rate(source) do
    {:ok, token} = Ecto.UUID.load(source.token)
    website_table = :"#{token}"
    log_table_info = :ets.info(website_table)

    case log_table_info do
      :undefined ->
        0

      _ ->
        Logflare.TableRateCounter.get_rate(website_table)
    end
  end

  def get_logs(table_id) do
    case :ets.info(table_id) do
      :undefined ->
        []

      _ ->
        List.flatten(:ets.match(table_id, {:_, :"$1"}))
    end
  end

  def get_latest_date(source) do
    {:ok, token} = Ecto.UUID.load(source.token)
    website_table = String.to_atom(token)

    case :ets.info(website_table) do
      :undefined ->
        0

      _ ->
        case :ets.last(website_table) do
          :"$end_of_table" ->
            0

          {timestamp, _unique_int, _monotime} ->
            timestamp
        end
    end
  end
end
