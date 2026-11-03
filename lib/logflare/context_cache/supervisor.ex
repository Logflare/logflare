defmodule Logflare.ContextCache.Supervisor do
  @moduledoc false

  use Supervisor

  require Logger

  alias Cainophile.Adapters.Postgres
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
  alias Logflare.Repo.ReplicaReconciler
  alias Logflare.Repo.Replicas
  alias Logflare.Rules
  alias Logflare.SavedSearches
  alias Logflare.Sources
  alias Logflare.SourceSchemas
  alias Logflare.SystemCache
  alias Logflare.TeamUsers
  alias Logflare.Users

  @wal_replica_key {__MODULE__, :wal_replica}
  @replica_disabled_handler_id {__MODULE__, :replica_disabled}

  def start_link(_) do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl Supervisor
  def init(_) do
    :telemetry.attach(
      @replica_disabled_handler_id,
      [:logflare, :repo, :replica_reconciler, :disabled],
      &__MODULE__.handle_replica_disabled/4,
      nil
    )

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
  Starts the Cainophile WAL subscriber, selecting its WAL source on every start.
  """
  @spec start_cainophile() :: GenServer.on_start()
  def start_cainophile do
    {replica, opts} = cainophile_options()
    :persistent_term.put(@wal_replica_key, replica)
    Postgres.start_link(opts)
  end

  @doc """
  Returns the followed replica key (`nil` for the primary) and the Cainophile options.

  With `prefer_read_replica`, the first read replica that is not disabled and passes
  `ReplicaReconciler.compare/1` is followed.
  """
  @spec cainophile_options() :: {ReplicaReconciler.replica_key() | nil, keyword()}
  def cainophile_options do
    cache_buster_config = Application.get_env(:logflare, CacheBuster)

    {replica, connection_config} =
      cache_buster_config[:prefer_read_replica]
      |> select_wal_replica(Application.get_env(:logflare, :read_replicas, []))
      |> wal_connection()

    {replica,
     [
       register: publisher_name(),
       epgsql: ConnectionOptions.prepare_epgsql(connection_config),
       slot: cache_buster_config[:replication_slot],
       wal_position: {"0", "0"},
       publications: cache_buster_config[:publications]
     ]}
  end

  @doc """
  Returns the replica key the WAL subscriber on this node follows, or `nil` for the primary.
  """
  @spec wal_replica() :: ReplicaReconciler.replica_key() | nil
  def wal_replica, do: :persistent_term.get(@wal_replica_key, nil)

  @doc false
  @spec handle_replica_disabled([atom()], map(), map(), term()) :: :ok
  def handle_replica_disabled(_event, _measurements, %{replica: replica}, _config) do
    with ^replica <- wal_replica(),
         pid when is_pid(pid) and node(pid) == node() <- GenServer.whereis(publisher_name()) do
      Logger.warning("Read replica disabled, restarting WAL subscriber on another source",
        replica: inspect(replica)
      )

      Process.exit(pid, :replica_disabled)
    end

    :ok
  end

  defp select_wal_replica(true, replicas), do: Enum.find(replicas, &wal_replica_in_sync?/1)
  defp select_wal_replica(_prefer_read_replica?, _replicas), do: nil

  defp wal_replica_in_sync?(entry) do
    key = ReplicaReconciler.replica_key(entry)

    with false <- ReplicaReconciler.disabled?(key),
         :ok <- ReplicaReconciler.compare(Replicas.repo(entry)) do
      true
    else
      result ->
        Logger.warning("Read replica not eligible to follow WAL for cache busting",
          replica: inspect(key),
          result: inspect(result)
        )

        false
    end
  end

  defp wal_connection(nil), do: {nil, Repo.config()}

  defp wal_connection(entry),
    do: {ReplicaReconciler.replica_key(entry), Replicas.connection_config(entry)}

  defp cainophile_child_spec do
    %{
      id: Postgres,
      restart: :permanent,
      type: :worker,
      start: {__MODULE__, :start_cainophile, []}
    }
  end
end
