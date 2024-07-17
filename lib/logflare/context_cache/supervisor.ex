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

  alias Logflare.Repo

  def start_link(_) do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @env Application.compile_env(:logflare, :env)

  @impl Supervisor
  def init(_) do
    res = Supervisor.init(get_children(@env), strategy: :one_for_one)
    res
  end

  defp get_children(:test) do
    [
      ContextCache,
      TeamUsers.Cache,
      Partners.Cache,
      Users.Cache,
      Backends.Cache,
      Sources.Cache,
      Billing.Cache,
      SourceSchemas.Cache,
      Auth.Cache
    ]
  end

  defp get_children(_) do
    get_children(:test) ++
      [
        ContextCache.TransactionBroadcaster,
        ContextCache.CacheBuster
      ]
  end

  @doc """
  Returns the publisher :via name used for syn registry.
  """
  def publisher_name do
    {:via, :syn, {:context_cache, Logflare.PgPublisher}}
    # {:global, Logflare.PgPublisher}
  end

  def maybe_start_cainophile do
    spec = cainophile_child_spec()
    Supervisor.start_child(__MODULE__, spec)
  end

  def cainophile_child_spec do
    hostname = ~c"#{Application.get_env(:logflare, Repo)[:hostname]}"
    username = Application.get_env(:logflare, Repo)[:username]
    password = Application.get_env(:logflare, Repo)[:password]
    database = Application.get_env(:logflare, Repo)[:database]
    port = Application.get_env(:logflare, Repo)[:port]

    slot = Application.get_env(:logflare, CacheBuster)[:replication_slot]
    publications = Application.get_env(:logflare, CacheBuster)[:publications]

    {
      Cainophile.Adapters.Postgres,
      register: publisher_name(),
      epgsql: %{
        host: hostname,
        port: port,
        username: username,
        database: database,
        password: password
      },
      slot: slot,
      wal_position: {"0", "0"},
      publications: publications
    }
  end
end
