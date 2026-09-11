defmodule Logflare.Backends.Spool.Buffer.Mem do
  @moduledoc """
  In-memory batch buffer for `Logflare.Backends.Spool.Partition` — appends
  accumulate as plain segments in memory, never written to local disk at
  all. This trades away local durability entirely: a node crash loses
  every not-yet-committed entry, which the WAL buffer exists specifically
  to avoid — an explicit choice (see `Logflare.Backends.spool_buffer/0`),
  not the default. Never touches local disk, so `on_commit_result/3` here
  is a no-op — there's no local write/roll failure of its own to report to
  `Logflare.Backends.Spool.Health`, and nothing to recover after a crash
  (`recover/1` is always `[]`, there's nothing durable here to find). A
  commit failure still reports to `Health`, same as the WAL buffer — that
  reporting happens uniformly in `Partition.settle_commit/3`, not here.

  Rolls once the accumulated raw byte count crosses a threshold
  (`mem_max_batch_bytes`, config), or the recurring flush timer fires,
  whichever comes first.
  """

  @behaviour Logflare.Backends.Spool.Buffer

  @default_max_batch_bytes 7 * 1024 * 1024

  @impl true
  def init(_opts) do
    spool_config = Application.get_env(:logflare, :spool, [])

    %{
      max_batch_bytes: Keyword.get(spool_config, :mem_max_batch_bytes, @default_max_batch_bytes),
      pending: [],
      pending_bytes: 0,
      pending_count: 0
    }
  end

  @impl true
  def append(state, segment, raw_byte_size, event_count) do
    state = %{
      state
      | pending: [{segment, raw_byte_size, event_count} | state.pending],
        pending_bytes: state.pending_bytes + raw_byte_size,
        pending_count: state.pending_count + event_count
    }

    {:ok, state}
  end

  @impl true
  def roll(%{pending: []} = state, _force), do: {:no_roll, state}

  def roll(state, force) do
    if force or over_threshold?(state) do
      # pending accumulates newest-first (append/4 prepends, O(1) rather than
      # appending to the list's tail on every call) — reversed here, once per
      # roll, so the concatenated body preserves the actual append order.
      body = state.pending |> Enum.reverse() |> Enum.map(&elem(&1, 0)) |> IO.iodata_to_binary()
      total_count = state.pending_count
      new_state = %{state | pending: [], pending_bytes: 0, pending_count: 0}
      {:ok, fn -> {:ok, body} end, nil, total_count, new_state}
    else
      {:no_roll, state}
    end
  end

  @impl true
  def on_commit_result(state, _context, _result), do: state

  @impl true
  def recover(state), do: {[], state}

  defp over_threshold?(state), do: state.pending_bytes >= state.max_batch_bytes
end
