defmodule Logflare.AccountCache do
  use GenServer

  require Logger

  import Ecto.Query, only: [from: 2]

  alias Logflare.Repo

  @refresh_every 1_000
  @table :account_cache

  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(state) do
    init_table()
    refresh()
    Logger.info("Account cache started!")
    {:ok, state}
  end

  def handle_info(:refresh, state) do
    insert_all_accounts()
    refresh()
    {:noreply, state}
  end

  # Public Interface

  @spec remove_account(String.t()) :: true
  def remove_account(api_key) do
    :ets.delete(@table, api_key)
  end

  @spec verify_account?(String.t()) :: BOOL
  def verify_account?(api_key) do
    case :ets.lookup(@table, api_key) do
      [{_api_key, _sources}] ->
        true

      [] ->
        false
    end
  end

  # Private Interface

  defp refresh() do
    Process.send_after(self(), :refresh, @refresh_every)
  end

  defp init_table() do
    :ets.new(@table, [:public, :named_table])

    insert_all_accounts()
  end

  defp insert_all_accounts() do
    api_keys =
      from(u in "users",
        select: %{
          api_key: u.api_key
        }
      )

    for key <- Repo.all(api_keys) do
      sources = []
      :ets.insert(@table, {key.api_key, sources})
    end
  end
end
