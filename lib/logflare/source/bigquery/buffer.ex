defmodule Logflare.Source.BigQuery.Buffer do
  @moduledoc false
  use GenServer
  alias Logflare.LogEvent, as: LE
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Source

  require Logger

  @broadcast_every 1_000

  def start_link(%RLS{source_id: source_id}) when is_atom(source_id) do
    GenServer.start_link(
      __MODULE__,
      %{
        source_id: source_id,
        buffer: :queue.new(),
        read_receipts: %{}
      },
      name: name(source_id)
    )
  end

  def init(state) do
    Logger.info("Table buffer started: #{state.source_id}")
    Process.flag(:trap_exit, true)

    check_buffer()
    {:ok, state}
  end

  def push(source_id, %LE{} = log_event) do
    GenServer.cast(name(source_id), {:push, log_event})
  end

  def pop(source_id) do
    GenServer.call(name(source_id), :pop)
  end

  def ack(source_id, log_event_id) do
    GenServer.call(name(source_id), {:ack, log_event_id})
  end

  def get_count(source_id) do
    GenServer.call(name(source_id), :get_count)
  end

  def handle_cast({:push, %LE{} = event}, state) do
    new_buffer = :queue.in(event, state.buffer)
    new_state = %{state | buffer: new_buffer}
    {:noreply, new_state}
  end

  def handle_call(:pop, _from, state) do
    case :queue.is_empty(state.buffer) do
      true ->
        {:reply, :empty, state}

      false ->
        {{:value, %LE{} = log_event}, new_buffer} = :queue.out(state.buffer)
        new_read_receipts = Map.put(state.read_receipts, log_event.id, log_event)

        new_state = %{state | buffer: new_buffer, read_receipts: new_read_receipts}
        {:reply, log_event, new_state}
    end
  end

  def handle_call({:ack, log_event_id}, _from, state) do
    case state.read_receipts == %{} do
      true ->
        {:reply, :empty, state}

      false ->
        {%LE{} = log_event, new_read_receipts} = Map.pop(state.read_receipts, log_event_id)

        new_state = %{state | read_receipts: new_read_receipts}
        {:reply, log_event, new_state}
    end
  end

  def handle_call(:get_count, _from, state) do
    count = :queue.len(state.buffer)
    {:reply, count, state}
  end

  def handle_info(:check_buffer, state) do
    Source.ChannelTopics.broadcast_buffer(state.source_id, :queue.len(state.buffer))
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

  defp name(source_id) do
    String.to_atom("#{source_id}" <> "-buffer")
  end
end
