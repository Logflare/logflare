defmodule Logflare.SourceData do
  alias Logflare.TableRateCounter

  def get_log_count(source) do
    log_table_info = :ets.info(String.to_atom(elem(Ecto.UUID.load(source.token), 1)))

    case log_table_info do
      :undefined ->
        0

      _ ->
        log_table_info[:size]
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

  defp get_rate_int(website_table) do
    case :ets.info(website_table) do
      :undefined ->
        0

      _ ->
        TableRateCounter.get_rate(website_table)
    end
  end
end
