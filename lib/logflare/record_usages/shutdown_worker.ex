defmodule Logflare.RecordUsages.ShutdownWorker do
  @moduledoc """
  Flushes local usage during graceful shutdown. Start after the cache and Repo,
  but before request handlers, so they stop before the final snapshot is taken.
  """

  use GenServer, shutdown: 15_000

  require Logger

  alias Logflare.RecordUsages

  @flush_timeout 10_000

  @spec start_link(term()) :: GenServer.on_start()
  def start_link(_options) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl GenServer
  @spec init(nil) :: {:ok, nil}
  def init(nil) do
    Process.flag(:trap_exit, true)
    {:ok, nil}
  end

  @impl GenServer
  @spec terminate(term(), nil) :: :ok
  def terminate(_reason, nil) do
    task = Task.async(&RecordUsages.flush/0)

    case Task.yield(task, @flush_timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, :ok} ->
        :ok

      {:exit, reason} ->
        Logger.warning("Record usage shutdown flush failed: #{inspect(reason)}")

      nil ->
        Logger.warning("Record usage shutdown flush timed out after #{@flush_timeout}ms")
    end
  end
end
