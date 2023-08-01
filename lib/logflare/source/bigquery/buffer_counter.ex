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

  @broadcast_every 5_000
  @max_buffer_len 5_000
  @pool_size Application.compile_env(:logflare, Logflare.PubSub)[:pool_size]

  def start_link(%RLS{source_id: source_uuid}) when is_atom(source_uuid) do
    GenServer.start_link(
      __MODULE__,
      %{
        source_uuid: source_uuid
      },
      name: name(source_uuid)
    )
  end

  def init(args) do
    ref = :counters.new(5, [:write_concurrency])
    {:ok, _} = Registry.register(Logflare.CounterRegistry, {__MODULE__, args.source_uuid}, ref)

    state = %{
      source_uuid: args.source_uuid,
      len_max: :counters.put(ref, len_max_idx(), @max_buffer_len),
      counter_ref: ref
    }

    Process.flag(:trap_exit, true)
    check_buffer()
    {:ok, state}
  end

  @doc """
  Takes a `batch` of `Broadway.Message`s, pushes them into a Broadway pipeline and increments the `BufferCounter` count.
  """

  @spec push_batch(%{source: %Source{}, batch: [%Broadway.Message{}, ...], count: integer()}) ::
          {:ok, map()} | {:error, :buffer_full}
  def push_batch(%{source: %Source{token: source_uuid}, batch: batch, count: count})
      when is_list(batch) and is_atom(source_uuid) do
    with {:ok, ref} <- lookup_counter(source_uuid),
         {:ok, resp} <- push_by(ref, {:push, count}) do
      Source.BigQuery.Pipeline.name(source_uuid)
      |> Broadway.push_messages(batch)

      {:ok, resp}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Wraps `LogEvent`s in a `Broadway.Message`, pushes log events into the Broadway pipeline and increments the `BufferCounter` count.
  """

  @spec push(LE.t()) :: {:ok, map()} | {:error, :buffer_full}
  def push(%LE{source: %Source{token: source_uuid}} = le) do
    with {:ok, ref} <- lookup_counter(source_uuid),
         {:ok, resp} <- push_by(ref, {:push, 1}) do
      batch = [
        %Broadway.Message{
          data: le,
          acknowledger: {Source.BigQuery.BufferProducer, source_uuid, nil}
        }
      ]

      Source.BigQuery.Pipeline.name(source_uuid)
      |> Broadway.push_messages(batch)

      {:ok, resp}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Decrements the actual buffer count. If we've got a successfull `ack` from Broadway it means
  we don't have the log event anymore.
  """

  @spec ack(atom(), UUID) :: {:ok, map()}
  def ack(source_uuid, log_event_id) when is_binary(log_event_id) do
    {:ok, ref} = source_uuid |> lookup_counter()

    ack_by(ref, {:ack, 1})
  end

  @spec ack(atom(), [%Broadway.Message{}]) :: {:ok, map()}
  def ack(source_uuid, log_events) when is_list(log_events) do
    count = Enum.count(log_events)

    {:ok, ref} = source_uuid |> lookup_counter()

    ack_by(ref, {:ack, count})
  end

  @doc """
  Gets the current count of the buffer.
  """

  @spec get_count(Source.t()) :: integer
  def get_count(%Source{token: source_uuid}) when is_atom(source_uuid) do
    {:ok, ref} = lookup_counter(source_uuid)

    :counters.get(ref, len_idx())
  end

  @doc """
  Sets the max length of a buffer.
  """

  @spec set_len_max(atom(), integer()) :: {:ok, map()}
  def set_len_max(source_uuid, max) when is_atom(source_uuid) do
    {:ok, ref} = lookup_counter(source_uuid)
    :counters.put(ref, len_max_idx(), max)
    max = :counters.get(ref, len_max_idx())

    {:ok, %{len_max: max}}
  end

  def lookup_counter(source_uuid) when is_atom(source_uuid) do
    case Registry.lookup(Logflare.CounterRegistry, {__MODULE__, source_uuid}) do
      [{_pid, counter_ref}] -> {:ok, counter_ref}
      _error -> {:error, :not_found}
    end
  end

  @doc """
  Name of our buffer.

  ## Examples

      iex> Logflare.Source.BigQuery.BufferCounter.name(:"36a9d6a3-f569-4f0b-b7a8-8289b4270e11")
      :"36a9d6a3-f569-4f0b-b7a8-8289b4270e11-buffer"

  """

  @spec name(atom() | integer()) :: atom
  def name(source_uuid) when is_atom(source_uuid) do
    String.to_atom("#{source_uuid}" <> "-buffer")
  end

  def push_by(ref, {:push, by}) do
    len = :counters.get(ref, len_idx())
    max_len = :counters.get(ref, len_max_idx())

    if len < max_len do
      :ok = :counters.add(ref, len_idx(), by)
      :ok = :counters.add(ref, pushed_idx(), by)

      {:ok, %{len: len + by}}
    else
      :ok = :counters.add(ref, discarded_idx(), by)
      {:error, :buffer_full}
    end
  end

  def ack_by(ref, {:ack, by}) do
    :ok = :counters.sub(ref, len_idx(), by)
    len = :counters.get(ref, len_idx())

    {:ok, %{len: len}}
  end

  def get_counts(source_uuid) do
    {:ok, ref} = lookup_counter(source_uuid)

    %{
      source_id: source_uuid,
      pushed: :counters.get(ref, pushed_idx()),
      acknowledged: :counters.get(ref, ackd_idx()),
      len: :counters.get(ref, len_idx()),
      len_max: :counters.get(ref, len_max_idx()),
      discarded: :counters.get(ref, discarded_idx())
    }
  end

  def handle_info(:check_buffer, state) do
    if Source.RateCounterServer.should_broadcast?(state.source_uuid) do
      broadcast_buffer(state.source_uuid)
    end

    check_buffer()

    {:noreply, state}
  end

  def terminate(reason, state) do
    # Do Shutdown Stuff
    Logger.info("Going Down - #{inspect(reason)} - #{__MODULE__}", %{source_id: state.source_uuid})

    reason
  end

  defp broadcast_buffer(source_uuid) when is_atom(source_uuid) do
    {:ok, ref} = lookup_counter(source_uuid)
    len = :counters.get(ref, len_idx())
    local_buffer = %{Node.self() => %{len: len}}

    shard = :erlang.phash2(source_uuid, @pool_size)

    Phoenix.PubSub.broadcast(
      Logflare.PubSub,
      "buffers:shard-#{shard}",
      {:buffers, source_uuid, local_buffer}
    )

    cluster_buffer = PubSubRates.Cache.get_cluster_buffers(source_uuid)

    payload = %{
      buffer: cluster_buffer,
      source_token: source_uuid
    }

    Source.ChannelTopics.broadcast_buffer(payload)
  end

  defp check_buffer() do
    Process.send_after(self(), :check_buffer, @broadcast_every)
  end

  defp pushed_idx(), do: 1
  defp ackd_idx(), do: 2
  defp len_idx(), do: 3
  defp len_max_idx(), do: 4
  defp discarded_idx(), do: 5
end
