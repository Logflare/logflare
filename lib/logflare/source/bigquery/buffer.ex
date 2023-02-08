defmodule Logflare.Source.BigQuery.BufferCounter do
  @moduledoc """
  Maintains a count of log events inside the Source.BigQuery.Pipeline Broadway pipeline.
  """

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

  @doc """
  Takes a `batch` of `Broadway.Message`s, pushes them into a Broadway pipeline and increments the `BufferCounter` count.
  """

  @spec push_batch(%{source: %Source{}, batch: [%Broadway.Message{}, ...], count: integer()}) ::
          :ok
  def push_batch(%{source: %Source{token: source_uuid}, batch: batch, count: count})
      when is_list(batch) do
    name = Source.BigQuery.Pipeline.name(source_uuid)

    GenServer.cast(name(source_uuid), {:push, count})
    Broadway.push_messages(name, batch)

    :ok
  end

  @doc """
  Wraps `LogEvent`s in a `Broadway.Message`, pushes log events into the Broadway pipeline and increments the `BufferCounter` count.
  """

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

  @doc """
  increments the count of `pop`s of the buffer.
  """

  @spec pop(atom(), integer()) :: :ok
  def pop(source_id, count) do
    GenServer.cast(name(source_id), {:pop, count})
  end

  @doc """
  Decrements the actual buffer count. If we've got a successfull `ack` from Broadway it means
  we don't have the log event anymore.
  """

  @spec ack(atom(), UUID) :: :ok
  def ack(source_id, _log_event_id) do
    GenServer.cast(name(source_id), {:ack, 1})
  end

  @doc """
  Gets the current count of the buffer.
  """

  @spec get_count(atom() | integer() | Source.t()) :: integer
  def get_count(%Source{id: source_id, token: source_uuid}, opts \\ [key: :token])
      when is_atom(source_uuid) and is_integer(source_id) do
    key = Keyword.get(opts, :key)

    case key do
      :token -> GenServer.call(name(source_uuid), :get_count)
      :id -> GenServer.call(name(source_id), :get_count)
    end
  end

  @doc """
  Name of our buffer.

  ## Examples

      iex> Logflare.Source.BigQuery.BufferCounter.name(:"36a9d6a3-f569-4f0b-b7a8-8289b4270e11")
      :"36a9d6a3-f569-4f0b-b7a8-8289b4270e11-buffer"

      iex> Logflare.Source.BigQuery.BufferCounter.name(2345)
      :"2345-buffer"

  """

  @spec name(atom() | integer()) :: atom
  def name(source_uuid) when is_atom(source_uuid) do
    String.to_atom("#{source_uuid}" <> "-buffer")
  end

  def name(source_id) when is_integer(source_id) do
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
