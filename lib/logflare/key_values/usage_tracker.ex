defmodule Logflare.KeyValues.UsageTracker do
  @moduledoc false
  use GenServer

  alias Logflare.KeyValues

  @buffers {:key_value_usage_buffer_0, :key_value_usage_buffer_1}
  @active_idx_key {__MODULE__, :active_idx_ref}
  @flush_interval :timer.seconds(30)
  @flush_chunk_size 5_000
  @drain_match_spec [{{:"$1"}, [], [:"$1"]}]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec touch(integer(), String.t()) :: :ok
  def touch(user_id, key) do
    ref = :persistent_term.get(@active_idx_key)
    table = elem(@buffers, :atomics.get(ref, 1))
    :ets.insert(table, {{user_id, key}})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @spec flush() :: :ok
  def flush do
    GenServer.call(__MODULE__, :flush)
  end

  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)

    ref = :atomics.new(1, signed: false)
    :persistent_term.put(@active_idx_key, ref)

    for table <- Tuple.to_list(@buffers) do
      :ets.new(table, [
        :set,
        :public,
        :named_table,
        {:write_concurrency, true},
        {:decentralized_counters, true},
        {:read_concurrency, false}
      ])
    end

    flush_interval = Keyword.get(opts, :flush_interval, default_flush_interval())
    chunk_size = Keyword.get(opts, :flush_chunk_size, default_flush_chunk_size())
    schedule_flush(flush_interval)

    {:ok, %{flush_interval: flush_interval, chunk_size: chunk_size}}
  end

  @impl GenServer
  def handle_call(:flush, _from, state) do
    do_flush(state.chunk_size)
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_info(:flush, state) do
    do_flush(state.chunk_size)
    schedule_flush(state.flush_interval)
    {:noreply, state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    do_flush(state.chunk_size)
    :ok
  end

  defp do_flush(chunk_size) do
    ref = :persistent_term.get(@active_idx_key)
    old_idx = :atomics.get(ref, 1)

    # (0 -> 1, 1 -> 0)
    :atomics.put(ref, 1, 1 - old_idx)
    old_table = elem(@buffers, old_idx)

    drain(:ets.select(old_table, @drain_match_spec, chunk_size), DateTime.utc_now())
    :ets.delete_all_objects(old_table)
    :ok
  end

  defp drain(:"$end_of_table", _now), do: :ok

  defp drain({pairs, continuation}, now) do
    KeyValues.bump_usages(pairs, now)
    drain(:ets.select(continuation), now)
  end

  defp schedule_flush(interval) do
    Process.send_after(self(), :flush, interval)
  end

  defp default_flush_interval do
    Application.get_env(:logflare, __MODULE__, [])
    |> Keyword.get(:flush_interval, @flush_interval)
  end

  defp default_flush_chunk_size do
    Application.get_env(:logflare, __MODULE__, [])
    |> Keyword.get(:flush_chunk_size, @flush_chunk_size)
  end
end
