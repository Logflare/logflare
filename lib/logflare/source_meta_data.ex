defmodule Logflare.SourceMetaData do
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
end
