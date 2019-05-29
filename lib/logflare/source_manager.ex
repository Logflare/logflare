defmodule Logflare.SourceManager do
  @moduledoc """
  Boots up a gen server per source table. Keeps a list of active tables in state.
  """

  use GenServer

  alias Logflare.Repo
  alias Logflare.SourceCounter
  alias Logflare.Google.BigQuery
  alias Logflare.SourceRecentLogs

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
    Logger.info("Table manager started!")

    query =
      from(s in "sources",
        select: %{
          token: s.token
        }
      )

    source_ids =
      Repo.all(query)
      |> Enum.map(fn s ->
        {:ok, source} = Ecto.UUID.Atom.load(s.token)
        source
      end)

    children =
      Enum.map(source_ids, fn source_id ->
        Supervisor.child_spec({SourceRecentLogs, source_id}, id: source_id, restart: :transient)
      end)

    Supervisor.start_link(children, strategy: :one_for_one)

    {:noreply, source_ids}
  end

  def handle_call({:create, source_id}, _from, state) do
    children = [
      Supervisor.child_spec({SourceRecentLogs, source_id}, id: source_id, restart: :transient)
    ]

    Supervisor.start_link(children, strategy: :one_for_one)

    state = Enum.uniq([source_id | state])
    {:reply, source_id, state}
  end

  def handle_call({:delete, source_id}, _from, state) do
    case Process.whereis(source_id) do
      nil ->
        {:reply, source_id, state}

      _ ->
        GenServer.stop(source_id)
        SourceCounter.delete(source_id)

        state = List.delete(state, source_id)
        {:reply, source_id, state}
    end
  end

  def terminate(reason, _state) do
    # Do Shutdown Stuff
    Logger.info("Going Down: #{__MODULE__}")
    reason
  end

  ## Public Functions

  def new_table(source_id) do
    GenServer.call(__MODULE__, {:create, source_id})
  end

  def delete_table(source_id) do
    GenServer.call(__MODULE__, {:delete, source_id})
    BigQuery.delete_table(source_id)

    {:ok, source_id}
  end

  def reset_table(source_id) do
    GenServer.call(__MODULE__, {:delete, source_id})
    GenServer.call(__MODULE__, {:create, source_id})

    {:ok, source_id}
  end

  def reset_all_user_tables(user) do
    sources = Repo.all(Ecto.assoc(user, :sources))
    Enum.each(sources, fn s -> reset_table(s.token) end)
  end

  def delete_all_tables() do
    state = :sys.get_state(Logflare.Main)

    Enum.map(
      state,
      fn t ->
        delete_table(t)
      end
    )

    {:ok}
  end

  def delete_all_empty_tables() do
    state = :sys.get_state(Logflare.Main)

    Enum.each(
      state,
      fn t ->
        first = :ets.first(t)

        case first == :"$end_of_table" do
          true ->
            delete_table(t)

          false ->
            :ok
        end
      end
    )

    {:ok}
  end
end
