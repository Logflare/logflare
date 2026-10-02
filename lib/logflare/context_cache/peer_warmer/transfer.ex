defmodule Logflare.ContextCache.PeerWarmer.Transfer do
  @moduledoc """
  Acknowledged, chunked transfer of cache entries from a peer node.

  The requesting process starts an exporter on the peer, which streams the cache and sends
  one chunk at a time, waiting for an ack before sending the next. The exporter stops when
  the requester goes down or stops acking, and the requester gives up at the deadline.
  """

  alias Logflare.Cluster.Utils, as: ClusterUtils
  alias Logflare.Utils.Tasks

  @chunk_size 500
  @ack_timeout to_timeout(second: 10)
  @rpc_timeout to_timeout(second: 5)

  @type import_fun :: ([term()] -> non_neg_integer())
  @type error :: :rpc_failed | :timeout | :peer_down

  @spec rpc_timeout() :: timeout()
  def rpc_timeout, do: @rpc_timeout

  @spec run(node(), module(), atom(), import_fun(), timeout()) ::
          {:ok, non_neg_integer()} | {:error, error()}
  def run(node, store, target, import_fun, timeout) do
    ref = make_ref()
    deadline = System.monotonic_time(:millisecond) + timeout
    args = [store, target, self(), ref]

    case ClusterUtils.erpc_multicall([node], __MODULE__, :start_export, args, @rpc_timeout) do
      [{^node, {:ok, {:ok, exporter}}}] ->
        monitor = Process.monitor(exporter)

        receive_chunks(
          %{ref: ref, exporter: exporter, monitor: monitor, deadline: deadline},
          import_fun,
          0
        )

      _error ->
        {:error, :rpc_failed}
    end
  end

  @doc false
  @spec start_export(module(), atom(), pid(), reference()) :: DynamicSupervisor.on_start_child()
  def start_export(store, target, requester, ref) do
    Tasks.start_child(fn -> export(store, target, requester, ref) end)
  end

  defp export(store, target, requester, ref) do
    monitor = Process.monitor(requester)

    target
    |> store.stream()
    |> Stream.chunk_every(@chunk_size)
    |> Enum.reduce_while(:ok, fn chunk, :ok ->
      send(requester, {ref, :chunk, chunk})
      await_ack(ref, monitor)
    end)
    |> case do
      :ok -> send(requester, {ref, :done})
      _stopped -> :ok
    end
  end

  defp await_ack(ref, monitor) do
    receive do
      {^ref, :ack} -> {:cont, :ok}
      {:DOWN, ^monitor, :process, _pid, _reason} -> {:halt, :requester_down}
    after
      @ack_timeout -> {:halt, :ack_timeout}
    end
  end

  defp receive_chunks(%{ref: ref, monitor: monitor} = transfer, import_fun, count) do
    remaining = max(transfer.deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {^ref, :chunk, entries} ->
        imported = import_fun.(entries)
        send(transfer.exporter, {ref, :ack})
        receive_chunks(transfer, import_fun, count + imported)

      {^ref, :done} ->
        Process.demonitor(monitor, [:flush])
        {:ok, count}

      {:DOWN, ^monitor, :process, _pid, _reason} ->
        {:error, :peer_down}
    after
      remaining ->
        Process.demonitor(monitor, [:flush])
        {:error, :timeout}
    end
  end
end
