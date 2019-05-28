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

  def start_link(source_ids \\ []) do
    GenServer.start_link(__MODULE__, source_ids, name: __MODULE__)
  end

  def init(source_ids) do
    persist()

    {:ok, source_ids, {:continue, :boot}}
  end

  ## Server

  def handle_continue(:boot, []) do
    Logger.info("Table manager started!")

    query =
      from(s in "sources",
        select: %{
          token: s.token
        }
      )

    sources = Repo.all(query)

    state =
      Enum.map(
        sources,
        fn s ->
          {:ok, source} = Ecto.UUID.Atom.load(s.token)
          source
        end
      )

    Enum.each(state, fn s ->
      SourceRecentLogs.start_link(s)
    end)

    {:noreply, state}
  end

  def handle_continue(:boot, source_ids) do
    Logger.info("Table manager started!")

    Enum.each(source_ids, fn s ->
      SourceRecentLogs.start_link(s)
    end)

    {:noreply, source_ids}
  end

  def handle_call({:create, source_id}, _from, state) do
    SourceRecentLogs.start_link(source_id)

    state = Enum.uniq([source_id | state])
    {:reply, source_id, state}
  end

  def handle_call({:stop, source_id}, _from, state) do
    case Process.whereis(source_id) do
      nil ->
        {:reply, source_id, state}

      _ ->
        source_id_string = Atom.to_string(source_id)
        tab_path = "tables/" <> source_id_string <> ".tab"

        GenServer.stop(source_id)

        SourceCounter.delete(source_id)
        File.rm(tab_path)
        state = List.delete(state, source_id)
        {:reply, source_id, state}
    end
  end

  def handle_info(:persist, state) do
    case File.stat("tables") do
      {:ok, _stats} ->
        persist_tables(state)

      {:error, _reason} ->
        File.mkdir("tables")
        persist_tables(state)
    end

    persist()
    {:noreply, state}
  end

  ## Public Functions

  def new_table(source_id) do
    GenServer.call(__MODULE__, {:create, source_id})
  end

  def delete_table(source_id) do
    GenServer.call(__MODULE__, {:stop, source_id})
    BigQuery.delete_table(source_id)

    {:ok, source_id}
  end

  def reset_table(source_id) do
    GenServer.call(__MODULE__, {:stop, source_id})
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

  ## Private Functions

  defp persist() do
    Process.send_after(self(), :persist, 60000)
  end

  defp persist_tables(state) do
    Enum.each(
      state,
      fn t ->
        tab_path = "tables/" <> Atom.to_string(t) <> ".tab"
        :ets.tab2file(t, String.to_charlist(tab_path))
      end
    )
  end
end
