defmodule Logflare.Source.BigQuery.Buffer do
  @moduledoc false
  use GenServer
  alias Logflare.LogEvent, as: LE
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Source
  alias Logflare.Sources
  alias Logflare.PubSubRates

  require Logger

  @broadcast_every 1_000

  def start_link(%RLS{source_id: source_id}) when is_atom(source_id) do
    GenServer.start_link(
      __MODULE__,
      %{
        source_id: source_id,
        buffer: :queue.new(),
        len: 0
      },
      name: name(source_id)
    )
  end

  def init(state) do
    Process.flag(:trap_exit, true)

    Sources.Buffers.put_buffer_len(state.source_id, state.len)

    {:ok, state, {:continue, :boot}}
  end

  def handle_continue(:boot, state) do
    check_buffer()

    {:noreply, state}
  end

  @spec push(LE.t()) :: :ok
  def push(%LE{source: %Source{token: source_id}} = le) do
    GenServer.cast(name(source_id), {:push, le})
  end

  @spec pop(atom | binary) :: any
  def pop(source_id) do
    GenServer.call(name(source_id), :pop, 60_000)
  end

  @spec ack(atom(), String.t()) :: {:ok, LE.t()}
  def ack(source_id, log_event_id) do
    # Don't need to run these through the genserver anymore
    case Sources.BuffersCache.take_read_receipt(log_event_id) do
      {:ok, nil} ->
        # Seeing a lot of these need to figure it out
        #
        # Logger.warn("Log event not found when acknowledged.",
        #   source_id: state.source_id,
        #   log_event_id: log_event_id
        # )

        {:error, :not_found}

      {:ok, %LE{} = log_event} ->
        {:ok, log_event}
    end
  end

  @spec get_count(atom | binary | Source.t()) :: integer
  def get_count(%Source{token: source_id}), do: get_count(source_id)
  def get_count(source_id), do: GenServer.call(name(source_id), :get_count)

  @spec get_log_events(atom | binary | Source.t()) :: [LE.t()]
  def get_log_events(%Source{token: source_id}), do: get_count(source_id)
  def get_log_events(source_id), do: GenServer.call(name(source_id), :get_log_events)

  def handle_cast({:push, %LE{} = event}, state) do
    new_buffer = :queue.in(event, state.buffer)
    len = state.len + 1

    Sources.Buffers.put_buffer_len(state.source_id, len)

    {:noreply, %{state | len: len, buffer: new_buffer}}
  end

  def handle_call(:pop, _from, state) do
    if :queue.is_empty(state.buffer) do
      Sources.Buffers.put_buffer_len(state.source_id, 0)

      {:reply, :empty, %{state | len: 0}}
    else
      {{:value, %LE{} = log_event}, new_buffer} = :queue.out(state.buffer)

      Sources.BuffersCache.put_read_receipt(log_event)

      len = state.len - 1

      Sources.Buffers.put_buffer_len(state.source_id, len)

      {:reply, log_event, %{state | len: len, buffer: new_buffer}}
    end
  end

  def handle_call(:get_count, _from, state) do
    count = :queue.len(state.buffer)
    {:reply, count, state}
  end

  def handle_call(:get_log_events, _from, state) do
    {les, le} = state.buffer

    {:reply, les ++ le, state}
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

  @spec name(atom | String.t()) :: atom
  def name(source_id) when is_atom(source_id) when is_binary(source_id) do
    String.to_atom("#{source_id}" <> "-buffer")
  end
end
