defmodule Logflare.TableBuffer do
  use GenServer

  alias Number.Delimit

  require Logger

  @broadcast_every 1_000

  def start_link(website_table) do
    GenServer.start_link(
      __MODULE__,
      %{
        source: website_table,
        buffer: :queue.new(),
        read_receipts: %{}
      },
      name: name(website_table)
    )
  end

  def init(state) do
    Logger.info("Table buffer started: #{state.source}")

    check_buffer()
    {:ok, state}
  end

  def push(website_table, event) do
    GenServer.cast(name(website_table), {:push, event})
  end

  def pop(website_table) do
    GenServer.call(name(website_table), :pop)
  end

  def ack(website_table, time_event) do
    GenServer.call(name(website_table), {:ack, time_event})
  end

  def get_count(website_table) do
    GenServer.call(name(website_table), :get_count)
  end

  def handle_cast({:push, event}, state) do
    new_buffer = :queue.in(event, state.buffer)
    new_state = %{state | buffer: new_buffer}
    {:noreply, new_state}
  end

  def handle_call(:pop, _from, state) do
    case :queue.is_empty(state.buffer) do
      true ->
        {:reply, :empty, state}

      false ->
        {event, new_buffer} = :queue.out(state.buffer)
        {:value, {time_event, data}} = event
        new_read_receipts = Map.put(state.read_receipts, time_event, data)

        new_state = %{state | buffer: new_buffer, read_receipts: new_read_receipts}
        {:reply, event, new_state}
    end
  end

  def handle_call({:ack, time_event}, _from, state) do
    case state.read_receipts == %{} do
      true ->
        {:reply, :empty, state}

      false ->
        {data, new_read_receipts} = Map.pop(state.read_receipts, time_event)
        event = {time_event, data}

        new_state = %{state | read_receipts: new_read_receipts}
        {:reply, event, new_state}
    end
  end

  def handle_call(:get_count, _from, state) do
    count = :queue.len(state.buffer)
    {:reply, count, state}
  end

  def handle_info(:check_buffer, state) do
    broadcast_buffer(state.source, :queue.len(state.buffer))
    check_buffer()
    {:noreply, state}
  end

  defp check_buffer() do
    Process.send_after(self(), :check_buffer, @broadcast_every)
  end

  defp broadcast_buffer(website_table, count) do
    website_table_string = Atom.to_string(website_table)

    payload = %{
      source_token: website_table_string,
      buffer: Delimit.number_to_delimited(count)
    }

    case :ets.info(LogflareWeb.Endpoint) do
      :undefined ->
        Logger.error("Endpoint not up yet!")

      _ ->
        LogflareWeb.Endpoint.broadcast(
          "dashboard:" <> website_table_string,
          "dashboard:#{website_table_string}:buffer",
          payload
        )
    end
  end

  defp name(website_table) do
    String.to_atom("#{website_table}" <> "-buffer")
  end
end
