defmodule Logflare.Backends.DefaultIngestRefresher do
  @moduledoc """
  Serializes account default ingest lookup refreshes on this node, partitioned by user id.

  Refreshes for a user run one at a time in arrival order, and each reads the primary
  database when it runs, so an earlier, slower read can never overwrite the result of a
  later one on this node.
  """

  use GenServer

  import Logflare.Utils.Guards

  require Logger

  alias Logflare.Backends

  @supervisor_name __MODULE__.Supervisor
  @refresh_timeout :timer.seconds(30)

  @spec supervisor_spec() :: Supervisor.module_spec()
  def supervisor_spec do
    {PartitionSupervisor, child_spec: __MODULE__, name: @supervisor_name}
  end

  @doc """
  Queues a refresh of the user's cached default ingest lookup on this node's partition for
  the user and waits for it to run. Returns `{:error, reason}` and leaves the cache untouched
  when the primary read fails with a database connection or Postgrex error.
  """
  @spec refresh(pos_integer()) :: :ok | {:error, Exception.t()}
  def refresh(user_id) when is_pos_integer(user_id) do
    GenServer.call(
      {:via, PartitionSupervisor, {@supervisor_name, user_id}},
      {:refresh, user_id},
      @refresh_timeout
    )
  end

  @spec start_link(term()) :: GenServer.on_start()
  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args)
  end

  @impl true
  def init(_init_args) do
    {:ok, %{}}
  end

  @impl true
  def handle_call({:refresh, user_id}, _from, state) do
    {:reply, do_refresh(user_id), state}
  end

  defp do_refresh(user_id) do
    case read_primary(user_id) do
      {:ok, fresh} -> cache_fresh(user_id, fresh)
      {:error, _reason} = error -> error
    end
  end

  defp read_primary(user_id) do
    {:ok, Backends.list_default_ingest_backends(user_id)}
  rescue
    error in [DBConnection.ConnectionError, Postgrex.Error] ->
      Logger.warning(
        "Default ingest lookup refresh failed: #{Exception.message(error)}",
        user_id: user_id
      )

      {:error, error}
  end

  defp cache_fresh(user_id, fresh) do
    key = {:list_default_ingest_backends, [user_id]}
    Enum.each(fresh, &Backends.maybe_start_consolidated_pipeline/1)

    Backends.Cache.fetch(key, fn -> fresh end)
    Backends.Cache.update(key, fresh)
  end
end
