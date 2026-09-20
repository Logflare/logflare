defmodule Logflare.Backends.Spool.DurableBuffer.Backends.RotatingWal.Worker do
  @moduledoc """
  Long-lived worker owning one inner `DurableBuffer.Backend` instance —
  a small, fixed pool of these lets `RotatingWal` ship rotated segments to
  the inner backend with bounded parallelism, while guaranteeing no inner
  backend's state is ever touched by more than one process at a time (each
  worker only ever threads its own inner_state through its own mailbox,
  one commit at a time).

  Started unlinked (`start/3`, not `start_link/3`) and monitored by its
  owner instead — a crashing upload must never take down the local WAL
  commit path. A sealed segment whose commit fails permanently is left on
  disk: the next partition restart's recovery scan picks it up again.
  """

  use GenServer

  require Logger

  @spec start(module(), term(), non_neg_integer()) :: GenServer.on_start()
  def start(inner_module, inner_config, sub_partition_index) do
    GenServer.start(__MODULE__, {inner_module, inner_config, sub_partition_index})
  end

  @spec commit_segment(pid(), Path.t()) :: :ok
  def commit_segment(worker, sealed_path) do
    GenServer.cast(worker, {:commit_segment, sealed_path})
  end

  @spec stop(pid()) :: :ok
  def stop(worker), do: GenServer.stop(worker)

  @impl true
  def init({inner_module, inner_config, sub_partition_index}) do
    {:ok, inner_state} = inner_module.open(inner_config, sub_partition_index)
    {:ok, %{inner_module: inner_module, inner_state: inner_state}}
  end

  @impl true
  def handle_cast({:commit_segment, sealed_path}, state) do
    case File.read(sealed_path) do
      {:ok, body} ->
        {:noreply, %{state | inner_state: commit_body(state, sealed_path, body)}}

      {:error, reason} ->
        Logger.error(
          "durable_buffer_rotating_wal: could not read sealed segment #{sealed_path}, " <>
            "leaving it for recovery: #{inspect(reason)}"
        )

        {:noreply, state}
    end
  end

  # {0, 0}: this worker pool ships file-addressed segments, not an
  # offset-indexed log, so there's no real span to report — the inner
  # backend (Backends.Cloud) ignores it.
  defp commit_body(state, sealed_path, body) do
    case state.inner_module.commit(state.inner_state, body, byte_size(body), {0, 0}) do
      {:ok, inner_state} ->
        File.rm(sealed_path)
        inner_state

      {:error, reason, inner_state} ->
        Logger.error(
          "durable_buffer_rotating_wal: inner commit failed for #{sealed_path}, leaving it " <>
            "on disk for the next recovery scan: #{inspect(reason)}"
        )

        inner_state
    end
  end
end
