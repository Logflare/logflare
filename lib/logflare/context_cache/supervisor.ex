defmodule Logflare.ContextCache.Supervisor do
  @moduledoc false

  use Supervisor

  alias Logflare.Backends
  alias Logflare.ContextCache.CacheBuster
  alias Logflare.ContextCache.CacheBusterWorker
  alias Logflare.Billing
  alias Logflare.ContextCache
  alias Logflare.Backends
  alias Logflare.SavedSearches
  alias Logflare.Sources
  alias Logflare.SourceSchemas
  alias Logflare.Users
  alias Logflare.TeamUsers
  alias Logflare.Rules
  alias Logflare.KeyValues
  alias Logflare.Partners
  alias Logflare.Auth
  alias Logflare.Endpoints
  alias Logflare.Repo
  alias Logflare.GenSingleton

  require Logger

  def start_link(_) do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl Supervisor
  def init(_) do
    Application.get_env(:logflare, :env)
    |> get_children()
    |> Supervisor.init(strategy: :one_for_one)
  end

  defp get_children(:test),
    do:
      list_caches() ++
        [
          {GenSingleton, child_spec: cainophile_child_spec()}
        ]

  defp get_children(_env) do
    list_caches() ++
      [
        ContextCache.TransactionBroadcaster,
        {GenSingleton, child_spec: cainophile_child_spec()}
      ] ++ buster_specs()
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
      {SavedSearches.Cache, :saved_searches}
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

  defp cainophile_child_spec do
    hostname = ~c"#{Application.get_env(:logflare, Repo)[:hostname]}"
    username = Application.get_env(:logflare, Repo)[:username]
    password = Application.get_env(:logflare, Repo)[:password]
    database = Application.get_env(:logflare, Repo)[:database]
    port = Application.get_env(:logflare, Repo)[:port]
    socket_options = Application.get_env(:logflare, Repo)[:socket_options]

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
             epgsql: %{
               host: hostname,
               port: port,
               username: username,
               database: database,
               password: password,
               tcp_opts: socket_options
             },
             slot: slot,
             wal_position: {"0", "0"},
             publications: publications
           ]
         ]}
    }
  end
end
