defmodule Logflare.Main do
  use GenServer

  alias Logflare.Repo

  import Ecto.Query, only: [from: 2]

  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  ## Client

  def new_table(website_table) do
    GenServer.call(__MODULE__, {:create, website_table})
  end

  def delete_table(website_table) do
    GenServer.call(__MODULE__, {:stop, website_table})
    {:ok, website_table}
  end

  def init(_state) do
    IO.puts "Genserver Started: #{__MODULE__}"

    query = from s in "sources",
          select: %{
            token: s.token,
          }

    sources = Repo.all(query)

    state =
      Enum.map(
        sources, fn(s) ->
          {:ok, source} = Ecto.UUID.load(s.token)
          String.to_atom(source)
        end
      )

    persist()
    {:ok, state}
  end

  ## Server

  def handle_call({:create, website_table}, _from, state) do
    Logflare.Table.start_link(website_table)
    state = Enum.uniq([website_table | state])
    {:reply, website_table, state}
  end

  def handle_call({:stop, website_table}, _from, state) do
    GenServer.stop(website_table)
    state = List.delete(state, website_table)
    {:reply, website_table, state}
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

  ## Private Functions

  defp persist() do
    Process.send_after(self(), :persist, 60000)
  end

  defp persist_tables(state) do
    Enum.each(
        state, fn(t) ->
          tab_path = "tables/" <> Atom.to_string(t) <> ".tab"
          :ets.tab2file(t, String.to_charlist(tab_path))
        end
      )
  end

  ## Public Functions

  def delete_all_tables() do
    state = :sys.get_state(Logflare.Main)
    Enum.map(
        state, fn(t) ->
          delete_table(t)
        end
      )
    {:ok}
  end

  def delete_all_empty_tables() do
    state = :sys.get_state(Logflare.Main)
    Enum.each(
        state, fn(t) ->
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
