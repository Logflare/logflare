defmodule Logflare.ContextCache.Supervisor do
  @moduledoc false

  use Supervisor

  alias Logflare.Backends
  alias Logflare.ContextCache.CacheBuster
  alias Logflare.ContextCache.CacheBusterWorker
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
  alias Logflare.GenSingleton

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
        {GenSingleton, child_spec: cainophile_child_spec()},
        {PartitionSupervisor, child_spec: CacheBusterWorker, name: CacheBusterWorker.Supervisor},
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
             fetch_current_wal_lsn: &fetch_current_wal_lsn/0,
             wal_flush_timeout: 30_000,
             slot: slot,
             wal_position: {"0", "0"},
             publications: publications
           ]
         ]}
    }
  end

  defp fetch_current_wal_lsn() do
    case Repo.query("SELECT pg_current_wal_lsn()::text", []) do
      {:ok, %Postgrex.Result{rows: [[lsn]]}} ->
        Cainophile.Adapters.Postgres.EpgsqlImplementation.parse_lsn_string(lsn)

      {:error, err} ->
        Logger.error("Error fetching current WAL LSN: #{inspect(err)}")
        {:error, :no_result}
    end
  end
end
