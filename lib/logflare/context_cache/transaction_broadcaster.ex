defmodule Logflare.ContextCache.TransactionBroadcaster do
  @moduledoc """
  Subscribes to cainophile and broadcasts all transactions
  """
  use GenServer

  require Logger

  alias Logflare.ContextCache
  alias Cainophile.Changes.Transaction

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  def init(args) do
    state = %{interval: Keyword.get(args, :interval, 5_000)}
    Process.send_after(self(), :try_subscribe, min(state.interval, 1_000))
    {:ok, state}
  end

  @doc """
  Sets the Logger level for this process. It's started with level :error.

  To debug wal records set process to level :debug and each transaction will be logged.
  """

  @spec set_log_level(Logger.levels()) :: :ok
  def set_log_level(level) when is_atom(level) do
    GenServer.call(__MODULE__, {:put_level, level})
  end

  def handle_call({:put_level, level}, _from, state) do
    :ok = Logger.put_process_level(self(), level)

    {:reply, :ok, state}
  end

  def handle_info(:try_subscribe, state) do
    pid = self()
    attempt_subscribe(pid)

    Process.send_after(self(), :try_subscribe, state.interval)
    {:noreply, state}
  end

  def handle_info(%Transaction{changes: []}, state), do: {:noreply, state |> dbg()}

  def handle_info(%Transaction{changes: _changes} = transaction, state) do
    Logger.debug("WAL record received from cainophile: #{inspect(transaction)}")
    # broadcast it
    Phoenix.PubSub.broadcast(Logflare.PubSub, "wal_transactions", transaction)
    {:noreply, state}
  end

  defp attempt_subscribe(pid) do
    try do
      ContextCache.Supervisor.maybe_start_cainophile()
      |> case do
        {:ok, pid} ->
          Logger.info(
            "Successfully started cainophile on #{inspect(Node.self())}, pid: #{inspect(pid)}"
          )

        {:error, _} ->
          :noop
      end

      ContextCache.Supervisor.publisher_name()
      |> Cainophile.Adapters.Postgres.subscribe(pid)
    rescue
      e ->
        Logger.warning("Could not subscribe to Cainophile, #{inspect(e)}")
    end
  end
end