defmodule Logflare.Source.BigQuery.Buffer do
  @moduledoc false
  use GenServer
  alias Logflare.LogEvent, as: LE
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Source
  alias Logflare.PubSubRates

  require Logger

  @broadcast_every 1_000

  def start_link(%RLS{source_id: source_id}) when is_atom(source_id) do
    GenServer.start_link(
      __MODULE__,
      %{
        source_id: source_id,
        popped: 0,
        pushed: 0,
        acknowledged: 0,
        len: 0
      },
      name: name(source_id)
    )
  end

  def init(state) do
    Process.flag(:trap_exit, true)
    check_buffer()
    {:ok, state}
  end

  @spec push(LE.t()) :: :ok
  def push(%LE{source: %Source{token: source_id}} = le) do
    name = Source.BigQuery.Pipeline.name(source_id)

    messages = [
      %Broadway.Message{
        data: le,
        acknowledger: {Source.BigQuery.BufferProducer, source_id, nil}
      }
    ]

    GenServer.cast(name(source_id), {:push, 1})
    Broadway.push_messages(name, messages)

    :ok
  end

  @spec pop(String.t(), integer()) :: :ok
  def pop(source_id, count) do
    GenServer.cast(name(source_id), {:pop, count})
  end

  @spec ack(String.t(), UUID) :: :ok
  def ack(source_id, _log_event_id) do
    GenServer.cast(name(source_id), {:ack, 1})
  end

  @spec get_count(atom | binary | Source.t()) :: integer
  def get_count(%Source{token: source_id}), do: get_count(source_id)

  def get_count(source_id), do: GenServer.call(name(source_id), :get_count)

  @spec name(atom | String.t()) :: atom
  def name(source_id) when is_atom(source_id) when is_binary(source_id) do
    String.to_atom("#{source_id}" <> "-buffer")
  end

  def handle_cast({:push, by}, state) do
    pushed = state.pushed + by
    len = state.len + by
    {:noreply, %{state | len: len, pushed: pushed}}
  end

  def handle_cast({:pop, by}, state) do
    popped = state.popped + by
    {:noreply, %{state | popped: popped}}
  end

  def handle_cast({:ack, by}, state) do
    ackd = state.acknowledged + by
    len = state.len - by
    {:noreply, %{state | len: len, acknowledged: ackd}}
  end

  def handle_call(:get_count, _from, state) do
    {:reply, state.len, state}
  end

  def handle_info(:check_buffer, state) do
    if Source.RateCounterServer.should_broadcast?(state.source_id) do
      broadcast_buffer(state)
    end

    check_buffer()

    {:noreply, state}
  end

  def terminate(reason, state) do
    # Do Shutdown Stuff
    Logger.info("Going Down - #{inspect(reason)} - #{__MODULE__}", %{source_id: state.source_id})
    reason
  end

  defp broadcast_buffer(state) do
    local_buffer = %{Node.self() => %{len: state.len}}

    Phoenix.PubSub.broadcast(
      Logflare.PubSub,
      "buffers",
      {:buffers, state.source_id, local_buffer}
    )

    cluster_buffer = PubSubRates.Cache.get_cluster_buffers(state.source_id)

    payload = %{
      buffer: cluster_buffer,
      source_token: state.source_id
    }

    Source.ChannelTopics.broadcast_buffer(payload)
  end

  defp check_buffer() do
    Process.send_after(self(), :check_buffer, @broadcast_every)
  end
end
