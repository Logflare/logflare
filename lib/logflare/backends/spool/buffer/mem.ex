defmodule Logflare.Backends.Spool.Buffer.Mem do
  @moduledoc """
  In-memory batch buffer for `Partition` — appends accumulate as plain
  segments in memory, never written to local disk. Rolls once the
  accumulated byte count crosses `mem_max_batch_bytes` (config), or the
  recurring flush timer fires, whichever comes first.
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
      # pending accumulates newest-first (O(1) prepend); reversed and
      # concatenated inside the thunk, deferred off this process.
      pending = state.pending
      total_count = state.pending_count
      new_state = %{state | pending: [], pending_bytes: 0, pending_count: 0}

      body_thunk = fn ->
        {:ok, pending |> Enum.reverse() |> Enum.map(&elem(&1, 0)) |> IO.iodata_to_binary()}
      end

      {:ok, body_thunk, nil, total_count, new_state}
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
