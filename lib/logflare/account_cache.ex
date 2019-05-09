defmodule Logflare.AccountCache do
  use GenServer

  require Logger

  alias Logflare.Repo
  alias Logflare.User
  alias Logflare.Source

  @refresh_every 1_000
  @table :account_cache

  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(state) do
    init_table()
    refresh()
    Logger.info("Account cache started!")
    Logflare.Google.CloudResourceManager.set_iam_policy!()
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

  @spec verify_account?(String.t()) :: boolean
  def verify_account?(api_key) do
    case :ets.lookup(@table, api_key) do
      [{_api_key, _sources}] ->
        true

      [] ->
        false
    end
  end

  @spec account_has_source?(String.t(), String.t()) :: boolean
  def account_has_source?(api_key, source_token) do
    [{_api_key, sources}] = :ets.lookup(@table, api_key)

    Enum.any?(sources, fn s -> s.token == source_token end)
  end

  @spec count_sources(String.t()) :: integer
  def count_sources(api_key) do
    [{_api_key, sources}] = :ets.lookup(@table, api_key)

    Enum.count(sources)
  end

  @spec get_source(String.t(), String.t()) :: Source.t()
  def get_source(api_key, source_token) do
    case :ets.lookup(@table, api_key) do
      [{_api_key, sources}] ->
        Enum.find(sources, fn source -> source.token == source_token end)

      [] ->
        nil
    end
  end

  @spec get_source_by_name(String.t(), String.t()) :: Source.t()
  def get_source_by_name(api_key, source_name) do
    [{_api_key, sources}] = :ets.lookup(@table, api_key)
    Enum.find(sources, fn source -> source.name == source_name end)
  end

  def get_rules(api_key, source_token) do
    [{_api_key, sources}] = :ets.lookup(@table, api_key)
    source = Enum.find(sources, fn source -> source.token == source_token end)
    source.rules
  end

  def update_account(api_key) do
    user = Repo.get_by(User, api_key: api_key) |> Repo.preload(:sources)

    sources =
      for source <- user.sources do
        Repo.preload(source, :rules)
      end

    :ets.insert(@table, {user.api_key, sources})
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
    accounts = Repo.all(User) |> Repo.preload(:sources)

    for account <- accounts do
      sources =
        Enum.map(account.sources, fn source ->
          Repo.preload(source, :rules)
        end)

      :ets.insert(@table, {account.api_key, sources})
    end
  end
end
