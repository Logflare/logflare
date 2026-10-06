defmodule Logflare.ContextCache.PeerWarmer.Transfer do
  @moduledoc """
  Acknowledged, chunked transfer of a context cache's entries from a peer node.

  The requesting process starts an exporter on the peer, which streams `c:Logflare.ContextCache.entries/0`
  and sends one chunk at a time, waiting for an ack before sending the next. The exporter stops when
  the requester goes down or stops acking, and the requester gives up at the deadline.
  """

  alias Logflare.Cluster.Utils, as: ClusterUtils
  alias Logflare.ContextCache
  alias Logflare.Utils.Tasks

  @chunk_size 500
  @ack_timeout to_timeout(second: 10)
  @rpc_timeout to_timeout(second: 5)

  @type import_fun :: ([ContextCache.entry()] -> non_neg_integer())
  @type error :: :rpc_failed | :timeout | :peer_down

  @spec rpc_timeout() :: timeout()
  def rpc_timeout, do: @rpc_timeout

  @spec run(node(), module(), import_fun(), timeout()) ::
          {:ok, non_neg_integer()} | {:error, error()}
  def run(node, cache, import_fun, timeout) do
    ref = make_ref()
    deadline = System.monotonic_time(:millisecond) + timeout
    args = [cache, self(), ref]

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
  @spec start_export(module(), pid(), reference()) :: DynamicSupervisor.on_start_child()
  def start_export(cache, requester, ref) do
    Tasks.start_child(fn -> export(cache, requester, ref) end)
  end

  defp export(cache, requester, ref) do
    monitor = Process.monitor(requester)

    cache.entries()
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
        ack_unless_late(transfer, import_fun, count + import_fun.(entries))

      {^ref, :done} ->
        Process.demonitor(monitor, [:flush])
        {:ok, count}

      {:DOWN, ^monitor, :process, _pid, _reason} ->
        {:error, :peer_down}
    after
      remaining -> time_out(transfer)
    end
  end

  defp ack_unless_late(transfer, import_fun, count) do
    if System.monotonic_time(:millisecond) < transfer.deadline do
      send(transfer.exporter, {transfer.ref, :ack})
      receive_chunks(transfer, import_fun, count)
    else
      time_out(transfer)
    end
  end

  defp time_out(transfer) do
    Process.demonitor(transfer.monitor, [:flush])
    {:error, :timeout}
  end
end
