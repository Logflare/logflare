defmodule Logflare.Logs do
  alias Logflare.Table
  alias Logflare.SourceCounter
  alias Logflare.SystemCounter
  alias Number.Delimit

  @system_counter :total_logs_logged

  @spec insert_or_push(atom(), {tuple(), map()}) :: true
  def insert_or_push(source_token, event) do
    if :ets.info(source_token) == :undefined do
      Table.push(source_token, event)
      true
    else
      :ets.insert(source_token, event)
    end
  end

  def broadcast_log_count(source_table) do
    {:ok, log_count} = SourceCounter.get_total_inserts(source_table)
    source_table_string = Atom.to_string(source_table)

    payload = %{
      log_count: Delimit.number_to_delimited(log_count),
      source_token: source_table_string
    }

    LogflareWeb.Endpoint.broadcast(
      "dashboard:" <> source_table_string,
      "dashboard:#{source_table_string}:log_count",
      payload
    )
  end

  def broadcast_total_log_count() do
    {:ok, log_count} = SystemCounter.log_count(@system_counter)
    payload = %{total_logs_logged: Delimit.number_to_delimited(log_count)}

    LogflareWeb.Endpoint.broadcast("everyone", "everyone:update", payload)
  end
end
