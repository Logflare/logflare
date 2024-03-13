defmodule Logflare.Source.BigQuery.BufferCounter do
  @moduledoc """
  Maintains a count of log events inside the Source.BigQuery.Pipeline Broadway pipeline.
  """

  use GenServer
  alias Logflare.LogEvent, as: LE
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Source
  alias Logflare.PubSubRates
  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.Source.BigQuery.Pipeline

  require Logger

  @broadcast_every 5_000
  @max_buffer_len 5_000

  def start_link(%RLS{source_id: token}),
    do:
      start_link(%{
        source_uuid: token,
        backend_token: nil,
        name: Source.Supervisor.via(__MODULE__, token)
      })

  def start_link(%{source_uuid: source_uuid, backend_token: _, name: name} = state)
      when is_atom(source_uuid) do
    GenServer.start_link(
      __MODULE__,
      state,
      name: name
    )
  end

  def init(args) do
    ref = :counters.new(5, [:write_concurrency])

    {:ok, _} =
      Registry.register(
        Logflare.CounterRegistry,
        {__MODULE__, args.source_uuid, args.backend_token},
        ref
      )

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
  Wraps `LogEvent`s in a `Broadway.Message`, pushes log events into the Broadway pipeline and increments the `BufferCounter` count.
  """

  @spec push(LE.t()) :: {:ok, map()} | {:error, :buffer_full}
  def push(%LE{source: %Source{token: source_uuid} = source} = le) do
    batch = [
      %Broadway.Message{
        data: le,
        acknowledger: {Source.BigQuery.BufferProducer, {source_uuid, nil}, nil}
      }
    ]

    push_batch(%{source: source, batch: batch, count: 1})
  end

  @doc """
  Takes a `batch` of `Broadway.Message`s, pushes them into a Broadway pipeline and increments the `BufferCounter` count.
  """

  @spec push_batch(Source.t(), Backend.t(), [Broadway.Message.t(), ...]) ::
          {:ok, map()} | {:error, :buffer_full}
  @spec push_batch(%{source: Source.t(), batch: [Broadway.Message.t(), ...], count: integer()}) ::
          {:ok, map()} | {:error, :buffer_full}
  def push_batch(%{source: %Source{token: source_uuid}, batch: batch, count: count})
      when is_list(batch) and is_atom(source_uuid) do
    with {:ok, ref} <- lookup_counter(source_uuid, nil),
         {:ok, resp} <- push_by(ref, count) do
      # v1 pipeline naming
      source_uuid
      |> Source.BigQuery.Pipeline.name()
      |> Broadway.push_messages(batch)

      {:ok, resp}
    end
  end

  # v2 pipeline ingestion
  def push_batch(%Source{} = source, %Backend{} = backend, messages) when is_list(messages) do
    with {:ok, ref} <- lookup_counter(source.token, backend.token),
         {:ok, resp} <- push_by(ref, Enum.count(messages)) do
      Backends.via_source(source, {Pipeline, backend.id})
      |> Broadway.push_messages(messages)

      {:ok, resp}
    end
  end

  # default backend with no backend record
  def push_batch(%Source{} = source, nil, messages) when is_list(messages) do
    with {:ok, ref} <- lookup_counter(source.token, nil),
         {:ok, resp} <- push_by(ref, Enum.count(messages)) do
      Backends.via_source(source, {Pipeline, nil})
      |> Broadway.push_messages(messages)

      {:ok, resp}
    end
  end

  @doc """
  Decrements the actual buffer count. If we've got a successfull `ack` from Broadway it means
  we don't have the log event anymore.
  """
  @spec ack(atom(), atom() | nil, binary()) :: {:ok, %{len: integer}}
  def ack(source_token, backend_token, log_event_id) when is_binary(log_event_id) do
    {:ok, ref} = lookup_counter(source_token, backend_token)
    ack_by(ref, 1)
  end

  @spec ack_batch(atom(), atom() | nil, [Broadway.Message.t()]) :: {:ok, map()}
  def ack_batch(source_token, backend_token, log_events) when is_list(log_events) do
    count = Enum.count(log_events)

    {:ok, ref} = lookup_counter(source_token, backend_token)

    ack_by(ref, count)
  end

  @spec push_by(:counters.counters_ref(), integer) ::
          {:error, :buffer_full} | {:ok, %{len: integer}}
  def push_by(ref, count) do
    len = :counters.get(ref, len_idx())
    max_len = :counters.get(ref, len_max_idx())

    if len < max_len do
      :ok = :counters.add(ref, len_idx(), count)
      :ok = :counters.add(ref, pushed_idx(), count)

      {:ok, %{len: len + count}}
    else
      :ok = :counters.add(ref, discarded_idx(), count)
      {:error, :buffer_full}
    end
  end

  @spec ack_by(:counters.counters_ref(), integer) :: {:ok, %{len: integer}}
  def ack_by(ref, count) do
    :ok = :counters.sub(ref, len_idx(), count)
    len = :counters.get(ref, len_idx())

    {:ok, %{len: len}}
  end

  @doc """
  Gets the current length of the buffer.
  """

  @spec len(Source.t()) :: integer
  def len(%Source{token: token}), do: len(token)

  def len(source_token, backend_token \\ nil) when is_atom(source_token) do
    {:ok, ref} = lookup_counter(source_token, backend_token)

    :counters.get(ref, len_idx())
  end

  @doc """
  Gets all the buffer counters for a source.
  """
  @spec get_counts(atom, atom | nil) :: map()
  def get_counts(source_token, backend_token \\ nil) do
    {:ok, ref} = lookup_counter(source_token, backend_token)

    %{
      # TODO: remove source_id to reduce confusion
      source_id: source_token,
      source_token: source_token,
      pushed: :counters.get(ref, pushed_idx()),
      acknowledged: :counters.get(ref, ackd_idx()),
      len: :counters.get(ref, len_idx()),
      len_max: :counters.get(ref, len_max_idx()),
      discarded: :counters.get(ref, discarded_idx())
    }
  end

  @doc """
  Sets the max length of a buffer. For tests.
  """

  @spec set_len_max(atom(), integer()) :: {:ok, map()}
  def set_len_max(source_token, max), do: set_len_max(source_token, nil, max)

  def set_len_max(source_token, backend_token, max) when is_atom(source_token) do
    {:ok, ref} = lookup_counter(source_token, backend_token)
    :ok = :counters.put(ref, len_max_idx(), max)
    max = :counters.get(ref, len_max_idx())

    {:ok, %{len_max: max}}
  end

  @doc """
  Looks up the counter reference from the Registry.
  """

  @spec lookup_counter(atom) :: {:ok, any} | {:error, :buffer_counter_not_found}
  def lookup_counter(source_token, backend_token \\ nil) when is_atom(source_token) do
    case Registry.lookup(Logflare.CounterRegistry, {__MODULE__, source_token, backend_token}) do
      [{_pid, counter_ref}] -> {:ok, counter_ref}
      _error -> {:error, :buffer_counter_not_found}
    end
  end

  def handle_info(:check_buffer, state) do
    if Source.RateCounterServer.should_broadcast?(state.source_uuid) do
      broadcast_buffer(state.source_uuid, state.backend_token)
    end

    check_buffer()

    {:noreply, state}
  end

  def terminate(reason, state) do
    # Do Shutdown Stuff
    # TODO: remove source_id metadata to reduce confusion
    Logger.info("Going Down - #{inspect(reason)} - #{__MODULE__}", %{
      source_id: state.source_uuid,
      source_token: state.source_uuid,
      backend_token: state.backend_token
    })

    reason
  end

  defp broadcast_buffer(source_token, backend_token) do
    pool_size = Application.get_env(:logflare, Logflare.PubSub)[:pool_size]
    {:ok, ref} = lookup_counter(source_token, backend_token)
    len = :counters.get(ref, len_idx())
    local_buffer = %{Node.self() => %{len: len}}

    shard = :erlang.phash2(source_token, pool_size)

    Phoenix.PubSub.broadcast(
      Logflare.PubSub,
      "buffers:shard-#{shard}",
      {:buffers, source_token, local_buffer}
    )

    cluster_buffer = PubSubRates.Cache.get_cluster_buffers(source_token)

    payload = %{
      buffer: cluster_buffer,
      source_token: source_token,
      backend_token: backend_token
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
