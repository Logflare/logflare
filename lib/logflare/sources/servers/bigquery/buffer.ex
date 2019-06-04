defmodule Logflare.Sources.Servers.BigQuery.Buffer do
  use GenServer

  alias Number.Delimit

  require Logger

  @broadcast_every 1_000

  def start_link(source_id) do
    GenServer.start_link(
      __MODULE__,
      %{
        source: source_id,
        buffer: :queue.new(),
        read_receipts: %{}
      },
      name: name(source_id)
    )
  end

  def init(state) do
    Logger.info("Table buffer started: #{state.source}")
    Process.flag(:trap_exit, true)

    check_buffer()
    {:ok, state}
  end

  def push(source_id, event) do
    GenServer.cast(name(source_id), {:push, event})
  end

  def pop(source_id) do
    GenServer.call(name(source_id), :pop)
  end

  def ack(source_id, time_event) do
    GenServer.call(name(source_id), {:ack, time_event})
  end

  def get_count(source_id) do
    GenServer.call(name(source_id), :get_count)
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

  def terminate(reason, _state) do
    # Do Shutdown Stuff
    Logger.info("Going Down: #{__MODULE__}")
    reason
  end

  defp check_buffer() do
    Process.send_after(self(), :check_buffer, @broadcast_every)
  end

  defp broadcast_buffer(source_id, count) do
    source_id_string = Atom.to_string(source_id)

    payload = %{
      source_token: source_id_string,
      buffer: Delimit.number_to_delimited(count)
    }

    case :ets.info(LogflareWeb.Endpoint) do
      :undefined ->
        Logger.error("Endpoint not up yet!")

      _ ->
        LogflareWeb.Endpoint.broadcast(
          "dashboard:" <> source_id_string,
          "dashboard:#{source_id_string}:buffer",
          payload
        )
    end
  end

  defp name(source_id) do
    String.to_atom("#{source_id}" <> "-buffer")
  end
end
