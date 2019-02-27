defmodule Logflare.TableRateCounter do
  @moduledoc """
  Establishes requests per second per source table. Watches the counters for source tables and periodically pulls them to establish
  events per second. Also handles storing those in the database.
  """
  use GenServer

  require Logger

  alias Logflare.TableCounter

  @rate_period 1_000

  def start_link(website_table, init_rate) do
    GenServer.start_link(__MODULE__, %{table: website_table, count: init_rate, current_rate: 0},
      name: name(website_table)
    )
  end

  def init(state) do
    Logger.info("Rate counter started: #{state.table}")
    put_current_rate()
    {:ok, state}
  end

  def get_rate(website_table) do
    name = name(website_table)
    GenServer.call(name, :rate)
  end

  def handle_call(:rate, _from, state) do
    rate = state.current_rate
    {:reply, rate, state}
  end

  def handle_info(:put_rate, state) do
    {:ok, current_count} = TableCounter.get_inserts(state.table)
    previous_count = state.count
    current_rate = current_count - previous_count
    broadcast_rate(state.table, current_rate)
    put_current_rate()
    {:noreply, %{table: state.table, count: current_count, current_rate: current_rate}}
  end

  defp put_current_rate() do
    Process.send_after(self(), :put_rate, @rate_period)
  end

  defp name(website_table) do
    String.to_atom("#{website_table}" <> "-rate")
  end

  defp broadcast_rate(website_table, rate) do
    website_table_string = Atom.to_string(website_table)
    payload = %{source_token: website_table_string, rate: rate}

    LogflareWeb.Endpoint.broadcast(
      "dashboard:" <> website_table_string,
      "dashboard:#{website_table_string}:rate",
      payload
    )
  end
end
