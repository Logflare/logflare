defmodule Logflare.Test.FakeDurableBufferBackend do
  @moduledoc """
  Minimal `DurableBuffer.Backend` for tests that need to observe/control
  what an inner backend does without touching real storage/queue mods —
  `commit/3` reports every call (with the `partition_index` it was
  opened with, so callers can assert on per-worker sub-indices) and
  succeeds or fails per the configured `commit_result`.
  """

  @behaviour DurableBuffer.Backend

  @impl true
  def init_config(opts) do
    %{
      test_pid: Keyword.fetch!(opts, :test_pid),
      commit_result: Keyword.get(opts, :commit_result, :ok)
    }
  end

  @impl true
  def open(config, partition_index),
    do: {:ok, %{config: config, partition_index: partition_index}}

  @impl true
  def commit(state, body, byte_size, _span) do
    send(state.config.test_pid, {:committed, state.partition_index, body, byte_size})

    case state.config.commit_result do
      :ok -> {:ok, state}
      {:error, reason} -> {:error, reason, state}
    end
  end

  @impl true
  def stream(_config, _partition_index), do: raise("not supported")

  @impl true
  def truncate(state, _next_offset), do: {:ok, state}

  @impl true
  def close(_state), do: :ok
end
