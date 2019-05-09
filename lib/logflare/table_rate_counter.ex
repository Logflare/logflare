defmodule Logflare.TableRateCounter do
  @moduledoc """
  Establishes requests per second per source table. Watches the counters for source tables and periodically pulls them to establish
  events per second. Also handles storing those in the database.
  """
  use GenServer

  require Logger

  alias Logflare.TableCounter
  alias Number.Delimit

  @rate_period 1_000
  @ets_table_name :table_rate_counters

  def start_link(source, init_count) do
    started_at = System.monotonic_time(:second)

    GenServer.start_link(
      __MODULE__,
      %{
        table: source,
        previous_count: init_count,
        current_rate: 0,
        begin_time: started_at,
        max_rate: 0
      },
      name: name(source)
    )
  end

  def init(state) do
    Logger.info("Rate counter started: #{state.table}")
    setup_ets_table(state)
    put_current_rate()

    {:ok, state}
  end

  def handle_info(:put_rate, state) do
    {:ok, current_count} = TableCounter.get_inserts(state.table)
    previous_count = state.previous_count
    current_rate = current_count - previous_count

    max_rate =
      case state.max_rate < current_rate do
        false ->
          state.max_rate

        true ->
          current_rate
      end

    time = System.monotonic_time(:second)
    time_passed = time - state.begin_time
    average_rate = Kernel.trunc(current_count / time_passed)

    payload = %{current_rate: current_rate, average_rate: average_rate, max_rate: max_rate}

    :ets.insert(@ets_table_name, {state.table, payload})

    broadcast_rate(state.table, current_rate, average_rate, max_rate)

    put_current_rate()

    {:noreply,
     %{
       table: state.table,
       previous_count: current_count,
       current_rate: current_rate,
       begin_time: state.begin_time,
       max_rate: max_rate
     }}
  end

  @spec get_rate(atom) :: integer
  def get_rate(source) do
    if :ets.info(@ets_table_name) == :undefined do
      0
    else
      data = :ets.lookup(@ets_table_name, source)
      data[source].current_rate
    end
  end

  @spec get_avg_rate(atom) :: integer
  def get_avg_rate(source) do
    if :ets.info(@ets_table_name) == :undefined do
      0
    else
      data = :ets.lookup(@ets_table_name, source)
      data[source].average_rate
    end
  end

  @spec get_max_rate(atom) :: integer
  def get_max_rate(source) do
    if :ets.info(@ets_table_name) == :undefined do
      0
    else
      data = :ets.lookup(@ets_table_name, source)
      data[source].max_rate
    end
  end

  defp setup_ets_table(state) do
    payload = %{current_rate: 0, average_rate: 0, max_rate: 0}

    if :ets.info(@ets_table_name) == :undefined do
      table_args = [:named_table, :public]
      :ets.new(@ets_table_name, table_args)
    end

    :ets.insert(@ets_table_name, {state.table, payload})
  end

  defp put_current_rate(rate_period \\ @rate_period) do
    Process.send_after(self(), :put_rate, rate_period)
  end

  defp name(source) do
    String.to_atom("#{source}" <> "-rate")
  end

  defp broadcast_rate(source, rate, average_rate, max_rate) do
    source_string = Atom.to_string(source)

    payload = %{
      source_token: source_string,
      rate: Delimit.number_to_delimited(rate),
      average_rate: Delimit.number_to_delimited(average_rate),
      max_rate: Delimit.number_to_delimited(max_rate)
    }

    case :ets.info(LogflareWeb.Endpoint) do
      :undefined ->
        Logger.error("Endpoint not up yet!")

      _ ->
        LogflareWeb.Endpoint.broadcast(
          "dashboard:" <> source_string,
          "dashboard:#{source_string}:rate",
          payload
        )
    end
  end
end
