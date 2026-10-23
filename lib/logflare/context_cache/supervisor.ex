defmodule Logflare.ContextCache.Supervisor do
  @moduledoc false

  use Supervisor

  alias Logflare.Auth
  alias Logflare.Backends
  alias Logflare.Billing
  alias Logflare.ContextCache
  alias Logflare.ContextCache.CacheBuster
  alias Logflare.ContextCache.CacheBusterWorker
  alias Logflare.ContextCache.Tombstones
  alias Logflare.Endpoints
  alias Logflare.GenSingleton
  alias Logflare.KeyValues
  alias Logflare.Partners
  alias Logflare.Repo
  alias Logflare.Repo.ConnectionOptions
  alias Logflare.Repo.Replicas
  alias Logflare.Rules
  alias Logflare.SavedSearches
  alias Logflare.Sources
  alias Logflare.SourceSchemas
  alias Logflare.SystemCache
  alias Logflare.TeamUsers
  alias Logflare.Users

  def start_link(_) do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl Supervisor
  def init(_) do
    Application.get_env(:logflare, :env)
    |> get_children()
    |> Supervisor.init(strategy: :one_for_one)
  end

  defp get_children(env) do
    caches = list_caches()

    maybe_transaction_broadcaster =
      if env != :test do
        ContextCache.TransactionBroadcaster
      end

    maybe_cainophile =
      if Application.get_env(:logflare, :enable_cainophile, true) do
        {GenSingleton, child_spec: cainophile_child_spec()}
      end

    maybe_busters =
      if env != :test do
        buster_specs()
      end

    caches ++
      List.wrap(maybe_transaction_broadcaster) ++
      List.wrap(maybe_cainophile) ++
      List.wrap(maybe_busters)
  end

  def buster_specs do
    [
      CacheBusterWorker.supervisor_spec(),
      ContextCache.CacheBuster
    ]
  end

  def list_caches_with_metrics do
    [
      {TeamUsers.Cache, :team_users},
      {Partners.Cache, :partners},
      {Users.Cache, :users},
      {Backends.Cache, :backends},
      {Sources.Cache, :sources},
      {Billing.Cache, :billing},
      {SourceSchemas.Cache, :source_schemas},
      {Auth.Cache, :auth},
      {Endpoints.Cache, :endpoints},
      {Rules.Cache, :rules},
      {KeyValues.Cache, :key_values},
      {SavedSearches.Cache, :saved_searches},
      {Tombstones.Cache, :context_cache_tombstones},
      {SystemCache, :system}
    ]
  end

  def list_caches do
    Enum.map(list_caches_with_metrics(), fn {cache, _} -> cache end)
  end

  @doc """
  Returns the publisher :via name used for syn registry.
  """
  def publisher_name do
    {:via, :syn, {:core, Logflare.PgPublisher}}
    # {:global, Logflare.PgPublisher}
  end

  @doc """
  Returns the connection configuration used to follow the WAL for cache busting.

  With `prefer_read_replica` enabled and at least one read replica configured, the first
  configured replica is used. Otherwise the primary is used.
  """
  @spec wal_connection_config() :: keyword()
  def wal_connection_config do
    prefer_read_replica? = Application.get_env(:logflare, CacheBuster)[:prefer_read_replica]

    wal_connection_config(
      prefer_read_replica?,
      Application.get_env(:logflare, :read_replicas, [])
    )
  end

  defp wal_connection_config(true, [replica | _]), do: Replicas.connection_config(replica)
  defp wal_connection_config(_prefer_read_replica?, _replicas), do: Repo.config()

  defp cainophile_child_spec do
    epgsql = ConnectionOptions.prepare_epgsql(wal_connection_config())

    slot = Application.get_env(:logflare, CacheBuster)[:replication_slot]
    publications = Application.get_env(:logflare, CacheBuster)[:publications]

    %{
      id: Cainophile.Adapters.Postgres,
      restart: :permanent,
      type: :worker,
      start:
        {Cainophile.Adapters.Postgres, :start_link,
         [
           [
             register: publisher_name(),
             epgsql: epgsql,
             slot: slot,
             wal_position: {"0", "0"},
             publications: publications
           ]
         ]}
    }
  end
end
