defmodule Logflare.ContextCache.PeerWarmer.Transfer do
  @moduledoc """
  Acknowledged, chunked transfer of a context cache's entries from a peer node.

  The requesting process asks the peer to start an exporter. The peer first applies the `guard`
  MFA, which decides whether it may serve the copy and returns metadata for the requester. The
  exporter then streams `c:Logflare.ContextCache.entries/0` and sends one chunk at a time, waiting
  for an ack before sending the next. Neither side waits long for the other: the exporter stops
  when the requester goes down or doesn't ack a chunk within 2 seconds, and the requester gives
  up when no chunk arrives within 2 seconds or at the deadline.
  """

  alias Logflare.Cluster.Utils, as: ClusterUtils
  alias Logflare.ContextCache
  alias Logflare.Utils.Tasks

  @chunk_size 500
  @ack_timeout to_timeout(second: 2)
  @chunk_timeout to_timeout(second: 2)
  @rpc_timeout to_timeout(second: 2)

  @type import_fun :: ([ContextCache.entry()] -> non_neg_integer())
  @type guard :: {module(), atom(), list()}
  @type error :: :rpc_failed | :timeout | :peer_down | term()

  @spec rpc_timeout() :: timeout()
  def rpc_timeout, do: @rpc_timeout

  @doc """
  Copies `cache` from `node`, returning the number of imported entries and the guard's metadata.
  A guard refusal is returned as is.
  """
  @spec run(node(), module(), guard(), import_fun(), timeout()) ::
          {:ok, non_neg_integer(), term()} | {:error, error()}
  def run(node, cache, guard, import_fun, timeout) do
    ref = make_ref()
    deadline = System.monotonic_time(:millisecond) + timeout
    args = [cache, guard, self(), ref]

    case ClusterUtils.erpc_call(node, __MODULE__, :start_export, args, @rpc_timeout) do
      {:ok, {:ok, exporter, meta}} ->
        monitor = Process.monitor(exporter)
        transfer = %{ref: ref, exporter: exporter, monitor: monitor, deadline: deadline}

        with {:ok, count} <- receive_chunks(transfer, import_fun, 0), do: {:ok, count, meta}

      {:ok, {:error, _reason} = refused} ->
        refused

      _error ->
        {:error, :rpc_failed}
    end
  end

  @doc false
  @spec start_export(module(), guard(), pid(), reference()) ::
          {:ok, pid(), term()} | {:error, term()}
  def start_export(cache, {module, fun, args}, requester, ref) do
    with {:ok, meta} <- apply(module, fun, args),
         {:ok, exporter} <- Tasks.start_child(fn -> export(cache, requester, ref) end) do
      {:ok, exporter, meta}
    end
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
      min(remaining, @chunk_timeout) -> time_out(transfer)
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
