defmodule Logflare.Source.BigQuery.BufferCounter do
  @moduledoc """
  Generic module for maintaining counts.
  Fully async and non-blocking.
  Must be referenced with a :via tuple.

  Used for maintaining counts inside a pipeline.
  """

  use GenServer
  alias Logflare.Source
  alias Logflare.PubSubRates

  require Logger

  @broadcast_every 2_000
  @max_buffer_len 5_000

  def start_link(args) when is_list(args) do
    name = Keyword.get(args, :name)
    GenServer.start_link(__MODULE__, args, name: name)
  end

  def init(args) do
    ref = :counters.new(5, [:write_concurrency])
    name = Keyword.get(args, :name)
    {:ok, _} = Registry.register(Logflare.CounterRegistry, name, ref)
    :counters.put(ref, len_max_idx(), @max_buffer_len)

    state = %{
      name: name,
      source_id: args[:source_id],
      backend_id: args[:backend_id],
      source_token: args[:source_token],
      backend_token: args[:backend_token],
      counter_ref: ref
    }

    Process.flag(:trap_exit, true)
    check_buffer()
    {:ok, state}
  end

  def inc({:via, _, _} = name, count) do
    with {:ok, ref} <- lookup_counter(name) do
      push_by(ref, count)
    end
  end

  def decr({:via, _, _} = name, count) do
    with {:ok, ref} <- lookup_counter(name) do
      ack_by(ref, count)
    end
  end

  @spec push_by(:counters.counters_ref(), integer) ::
          {:error, :buffer_full} | {:ok, %{len: integer}}
  def push_by(ref, count) do
    len = :counters.get(ref, len_idx())
    max_len = :counters.get(ref, len_max_idx())

    if len < max_len do
      :ok = :counters.add(ref, len_idx(), count)
      :ok = :counters.add(ref, pushed_idx(), count)

      {:ok, len + count}
    else
      :ok = :counters.add(ref, discarded_idx(), count)
      {:error, :buffer_full}
    end
  end

  @spec ack_by(:counters.counters_ref(), integer) :: {:ok, %{len: integer}}
  def ack_by(ref, count) do
    :ok = :counters.sub(ref, len_idx(), count)
    len = :counters.get(ref, len_idx())

    {:ok, len}
  end

  @doc """
  Gets the current length of the buffer.
  """

  @spec len(tuple()) :: integer
  def len(name) do
    {:ok, ref} = lookup_counter(name)
    :counters.get(ref, len_idx())
  end

  @doc """
  Gets all the buffer counters for a source.
  """
  @spec get_counts(tuple()) :: map()
  def get_counts({:via, _, _} = name) do
    {:ok, ref} = lookup_counter(name)

    %{
      pushed: :counters.get(ref, pushed_idx()),
      acknowledged: :counters.get(ref, ackd_idx()),
      len: :counters.get(ref, len_idx()),
      len_max: :counters.get(ref, len_max_idx()),
      discarded: :counters.get(ref, discarded_idx())
    }
  end

  @doc """
  Looks up the counter reference from the Registry.
  """

  @spec lookup_counter(tuple) :: {:ok, any} | {:error, :buffer_counter_not_found}
  def lookup_counter(via) when is_tuple(via) do
    case Registry.lookup(Logflare.CounterRegistry, via) do
      [{_pid, counter_ref}] -> {:ok, counter_ref}
      _error -> {:error, :buffer_counter_not_found}
    end
  end

  def handle_info(:check_buffer, state) do
    if Source.RateCounterServer.should_broadcast?(state.source_token) do
      pool_size = Application.get_env(:logflare, Logflare.PubSub)[:pool_size]
      {:ok, ref} = lookup_counter(state.name)
      len = :counters.get(ref, len_idx())
      local_buffer = %{Node.self() => %{len: len}}

      shard = :erlang.phash2(state.source_token, pool_size)

      Phoenix.PubSub.broadcast(
        Logflare.PubSub,
        "buffers:shard-#{shard}",
        {:buffers, state.source_token, local_buffer}
      )

      cluster_buffer = PubSubRates.Cache.get_cluster_buffers(state.source_token)

      payload = %{
        buffer: cluster_buffer,
        source_token: state.source_token,
        backend_token: state.backend_token
      }

      Source.ChannelTopics.broadcast_buffer(payload)
    end

    check_buffer()

    {:noreply, state}
  end

  def terminate(reason, state) do
    # Do Shutdown Stuff
    # TODO: remove source_id metadata to reduce confusion
    Logger.info("Going Down - #{inspect(reason)} - #{__MODULE__}", %{
      source_id: state.source_token,
      source_token: state.source_token,
      backend_token: state.backend_token
    })

    reason
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
