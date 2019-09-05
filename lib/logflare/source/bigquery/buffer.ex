defmodule Logflare.Source.BigQuery.Buffer do
  @moduledoc false
  use GenServer
  alias Logflare.LogEvent, as: LE
  alias Logflare.Source.ChannelTopics, as: CT
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Source

  require Logger

  @broadcast_every 250

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
    Logger.info("BigQuery.Buffer started: #{state.source_id}")
    Process.flag(:trap_exit, true)

    init_metadata = %{source_token: "#{state.source_id}", buffer: 0}

    Phoenix.Tracker.track(
      Logflare.Tracker,
      self(),
      name(state.source_id),
      Node.self(),
      init_metadata
    )

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

        if :queue.is_empty(state.buffer) && new_read_receipts == %{} do
          {:reply, log_event, new_state, :hibernate}
        else
          {:reply, log_event, new_state}
        end
    end
  end

  def handle_call(:get_count, _from, state) do
    count = :queue.len(state.buffer)
    {:reply, count, state}
  end

  def handle_info(:check_buffer, state) do
    update_tracker(state)
    broadcast_buffer(state)
    check_buffer()

    {:noreply, state}
  end

  def terminate(reason, _state) do
    # Do Shutdown Stuff
    Logger.info("Going Down: #{__MODULE__}")
    reason
  end

  defp broadcast_buffer(state) do
    payload =
      Phoenix.Tracker.list(Logflare.Tracker, name(state.source_id))
      |> merge_metadata

    Source.ChannelTopics.broadcast_buffer(payload)
  end

  defp update_tracker(state) do
    pid = Process.whereis(name(state.source_id))
    payload = %{source_token: state.source_id, buffer: :queue.len(state.buffer)}

    Phoenix.Tracker.update(Logflare.Tracker, pid, name(state.source_id), Node.self(), payload)
  end

  def merge_metadata(list) do
    payload = {:noop, %{buffer: 0}}

    {:noop, data} =
      Enum.reduce(list, payload, fn {_, y}, {_, acc} ->
        buffer = y.buffer + acc.buffer

        {:noop, %{y | buffer: buffer}}
      end)

    data
  end

  defp check_buffer() do
    Process.send_after(self(), :check_buffer, @broadcast_every)
  end

  defp name(source_id) do
    String.to_atom("#{source_id}" <> "-buffer")
  end
end
