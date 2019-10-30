defmodule Logflare.Source.Supervisor do
  @moduledoc """
  Boots up a gen server per source table. Keeps a list of active tables in state.
  """

  use GenServer

  alias Logflare.Repo
  alias Logflare.Sources.Counters
  alias Logflare.Google.BigQuery
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Cluster

  import Ecto.Query, only: [from: 2]

  require Logger

  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(source_ids) do
    Process.flag(:trap_exit, true)
    {:ok, source_ids, {:continue, :boot}}
  end

  ## Server

  def handle_continue(:boot, _source_ids) do
    query =
      from(s in "sources",
        select: %{
          token: s.token
        }
      )

    source_ids =
      query
      |> Repo.all()
      |> Enum.map(fn s ->
        {:ok, source} = Ecto.UUID.Atom.load(s.token)
        source
      end)

    # Rate limit is 100/second
    Enum.map(source_ids, fn source_id ->
      rls = %RLS{source_id: source_id}
      Supervisor.child_spec({RLS, rls}, id: source_id, restart: :transient)
    end)
    |> Enum.chunk_every(100)
    |> Enum.each(fn x ->
      Supervisor.start_link(x, strategy: :one_for_one)
      Process.sleep(1_000)
    end)

    {:noreply, source_ids}
  end

  def handle_call({:create, source_id}, _from, state) do
    case create_source(source_id) do
      {:ok, _pid} ->
        state = Enum.uniq([source_id | state])
        {:reply, source_id, state}

      {:error, _reason} ->
        Logger.error("Failed to start RecentLogsServer: #{source_id}")

        {:reply, source_id, state}
    end
  end

  def handle_call({:delete, source_id}, _from, state) do
    case Process.whereis(source_id) do
      nil ->
        {:reply, source_id, state}

      _ ->
        send(source_id, {:stop_please, :shutdown})
        Counters.delete(source_id)

        state = List.delete(state, source_id)
        {:reply, source_id, state}
    end
  end

  def handle_cast({:restart, source_id}, state) do
    case Process.whereis(source_id) do
      nil ->
        case create_source(source_id) do
          {:ok, _pid} ->
            state = Enum.uniq([source_id | state])
            {:noreply, state}

          {:error, _reason} ->
            Logger.error("Failed to start RecentLogsServer: #{source_id}")

            {:noreply, state}
        end

        {:noreply, state}

      _ ->
        send(source_id, {:stop_please, :shutdown})

        Process.sleep(1_000)

        case create_source(source_id) do
          {:ok, _pid} ->
            state = Enum.uniq([source_id | state])
            {:noreply, state}

          {:error, _reason} ->
            Logger.error("Failed to start RecentLogsServer: #{source_id}")

            {:noreply, state}
        end
    end
  end

  def terminate(reason, _state) do
    # Do Shutdown Stuff
    Logger.info("Going Down: #{__MODULE__}")
    reason
  end

  ## Public Functions

  def new_source(source_id) do
    GenServer.multi_call(Cluster.Utils.node_list_all(), __MODULE__, {:create, source_id})
  end

  def delete_source(source_id) do
    GenServer.multi_call(Cluster.Utils.node_list_all(), __MODULE__, {:delete, source_id})
    BigQuery.delete_table(source_id)

    {:ok, source_id}
  end

  def reset_source(source_id) do
    GenServer.abcast(Cluster.Utils.node_list_all(), __MODULE__, {:restart, source_id})

    {:ok, source_id}
  end

  def reset_all_user_sources(user) do
    sources = Repo.all(Ecto.assoc(user, :sources))
    Enum.each(sources, fn s -> reset_source(s.token) end)
  end

  defp create_source(source_id) do
    rls = %RLS{source_id: source_id}

    children = [
      Supervisor.child_spec({RLS, rls}, id: source_id, restart: :transient)
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
