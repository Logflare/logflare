defmodule Logflare.ContextCache.Supervisor do
  @moduledoc false

  use Supervisor

  alias Logflare.Backends
  alias Logflare.ContextCache.CacheBuster
  alias Logflare.Billing
  alias Logflare.ContextCache
  alias Logflare.Backends
  alias Logflare.Sources
  alias Logflare.SourceSchemas
  alias Logflare.Users
  alias Logflare.TeamUsers
  alias Logflare.Partners
  alias Logflare.Auth
  alias Logflare.Endpoints
  alias Logflare.Repo

  def start_link(_) do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @env Application.compile_env(:logflare, :env)

  @impl Supervisor
  def init(_) do
    Supervisor.init(get_children(@env), strategy: :one_for_one)
  end

  defp get_children(:test), do: list_caches()

  defp get_children(_env) do
    list_caches() ++
      [
        ContextCache.TransactionBroadcaster,
        ContextCache.CacheBuster
      ]
  end

  def list_caches do
    [
      TeamUsers.Cache,
      Partners.Cache,
      Users.Cache,
      Backends.Cache,
      Sources.Cache,
      Billing.Cache,
      SourceSchemas.Cache,
      Auth.Cache,
      Endpoints.Cache
    ]
  end

  @doc """
  Returns the publisher :via name used for syn registry.
  """
  def publisher_name do
    {:via, :syn, {:core, Logflare.PgPublisher}}
    # {:global, Logflare.PgPublisher}
  end

  @doc """
  Attempts to start a cainophile child in the ContextCache.Supervisor.
  If it already exists, it will return with an error tuple.
  """
  def maybe_start_cainophile do
    spec = cainophile_child_spec()
    Supervisor.start_child(__MODULE__, spec)
  end

  def remove_cainophile do
    Supervisor.terminate_child(__MODULE__, Cainophile.Adapters.Postgres)
    Supervisor.delete_child(__MODULE__, Cainophile.Adapters.Postgres)
    :ok
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
