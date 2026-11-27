defmodule Logflare.ContextCache.FlushConfirmer do
  @moduledoc """
  Periodically confirms a flushed WAL position on a logical replication connection.

  Each tick confirms the server's WAL flush position read on the previous tick, so the slot
  advances even when no published transaction commits. The confirmed position can be ahead
  of what the subscriber has processed, which is only safe for temporary slots.

  Opens its own connection with the replication connection's options and stops when the
  replication connection exits.
  """

  use GenServer

  require Logger

  @type option :: {:replication, pid()} | {:epgsql, map()} | {:interval, pos_integer()}

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl GenServer
  def init(opts) do
    replication = Keyword.fetch!(opts, :replication)
    interval = Keyword.fetch!(opts, :interval)

    case :epgsql.connect(Keyword.fetch!(opts, :epgsql)) do
      {:ok, connection} ->
        Process.monitor(replication)
        Process.send_after(self(), :confirm, interval)

        {:ok,
         %{
           replication: replication,
           connection: connection,
           interval: interval,
           flushed_lsn: flushed_lsn(connection)
         }}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_info(:confirm, state) do
    confirm(state.replication, state.flushed_lsn)
    Process.send_after(self(), :confirm, state.interval)
    {:noreply, %{state | flushed_lsn: flushed_lsn(state.connection)}}
  end

  def handle_info(
        {:DOWN, _ref, :process, replication, _reason},
        %{replication: replication} = state
      ) do
    :epgsql.close(state.connection)
    {:stop, :normal, state}
  end

  defp confirm(_replication, nil), do: :ok
  defp confirm(replication, lsn), do: :epgsql.standby_status_update(replication, lsn, lsn)

  defp flushed_lsn(connection) do
    case :epgsql.equery(
           connection,
           "SELECT (pg_current_wal_flush_lsn() - '0/0'::pg_lsn)::bigint",
           []
         ) do
      {:ok, _columns, [{lsn}]} ->
        lsn

      {:error, reason} ->
        Logger.warning("Could not read the WAL flush position: #{inspect(reason)}")
        nil
    end
  end
end
