defmodule Logflare.Backends do
  @moduledoc false

  import Ecto.Query
  import Logflare.Utils.Guards

  require Logger

  alias Ecto.Changeset
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Backend
  alias Logflare.Backends.BackendRegistry
  alias Logflare.Backends.ConsolidatedSup
  alias Logflare.Backends.DefaultIngestRefresher
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.SourceRegistry
  alias Logflare.Backends.SourceSupStarts
  alias Logflare.Backends.SourcesSup
  alias Logflare.Backends.SourceSup
  alias Logflare.Backends.Spool.DurableBuffer.Supervisor, as: SpoolDurableBufferSup
  alias Logflare.Backends.Spool.Encoder, as: SpoolEncoder
  alias Logflare.Backends.Spool.Health, as: SpoolHealth
  alias Logflare.ContextCache
  alias Logflare.Cluster
  alias Logflare.LogEvent
  alias Logflare.PubSubRates
  alias Logflare.Repo
  alias Logflare.Rules.Rule
  alias Logflare.SingleTenant
  alias Logflare.Sources
  alias Logflare.Sources.Counters
  alias Logflare.Sources.Source
  alias Logflare.Sources.Source.RateSampler
  alias Logflare.Sources.SourceRouter
  alias Logflare.SystemMetrics
  alias Logflare.Teams
  alias Logflare.TeamUsers.TeamUser
  alias Logflare.User

  defdelegate child_spec(arg), to: __MODULE__.Supervisor

  @max_future_event_us 1 * 3_600 * 1_000_000
  @max_pending_buffer_len_per_queue IngestEventQueue.max_queue_size()
  @default_source_sup_start_timeout :timer.seconds(30)
  @max_pipeline_swap_delay_ms 5_000

  @type one_or_list_or_nil :: Backend.t() | [Backend.t()] | nil

  @doc """
  Retrieves the hardcoded max pending buffer length of an individual queue
  """
  @spec max_buffer_queue_len() :: non_neg_integer()
  def max_buffer_queue_len, do: @max_pending_buffer_len_per_queue

  @doc """
  Returns the maximum time, in microseconds, that an event's timestamp may be in
  the future and still be accepted at ingestion. Events further ahead than this
  are dropped.

  There is no equivalent global lower bound. Dropping events for being too old is
  now a backend-specific concern (_see `Logflare.Backends.Adaptor.ClickHouseAdaptor.pre_ingest/3`_)
  """
  @spec max_future_event_us() :: non_neg_integer()
  def max_future_event_us, do: @max_future_event_us

  @doc """
  Lists `Backend`s for a given source.
  """
  @spec list_backends(keyword()) :: [Backend.t()]
  def list_backends(filters) when is_list(filters) do
    Backend
    |> filter_backends(filters)
    |> Repo.all()
    |> Enum.map(fn sb -> typecast_config_string_map_to_atom_map(sb) end)
  end

  defp filter_backends(query, filters) do
    {ingesting?, filters} = Keyword.pop(filters, :ingesting, false)
    {limit, filters} = Keyword.pop(filters, :limit)

    filters
    |> Enum.reduce(query, fn
      {:types, types}, q when is_list(types) ->
        where(q, [b], b.type in ^types)

      # filter down to backends of this source
      {:source_id, id}, q ->
        join(q, :inner, [b], s in assoc(b, :sources), on: s.id == ^id)

      # filter down to backends with rules destinations
      {:rules_source_id, source_id}, q ->
        join(q, :inner, [b], r in assoc(b, :rules), on: r.source_id == ^source_id)

      {:user_id, id}, q ->
        where(q, [b], b.user_id == ^id)

      {:type, type}, q when is_atom(type) ->
        where(q, [b], b.type == ^type)

      {:metadata, %{} = metadata}, q ->
        normalized =
          Enum.into(metadata, %{}, fn {k, v} ->
            {k, if(v in ["true", "false"], do: String.to_existing_atom(v), else: v)}
          end)

        where(q, [b], b.metadata == ^normalized)

      # filter by `default_ingest?` flag
      {:default_ingest?, true}, q ->
        where(q, [b], b.default_ingest? == true)

      {:has_sources_or_rules, true}, q ->
        q
        |> join(:left, [b], sb in "sources_backends", on: sb.backend_id == b.id)
        |> join(:left, [b], r in Rule, on: r.backend_id == b.id)
        |> where(
          [b, ..., sb, r],
          not is_nil(sb.backend_id) or not is_nil(r.backend_id) or b.default_ingest? == true
        )
        |> distinct_backends()

      _, q ->
        q
    end)
    |> maybe_filter_ingesting(ingesting?)
    |> maybe_limit(limit)
  end

  defp maybe_filter_ingesting(query, true) do
    backend_ids =
      query
      |> exclude(:distinct)
      |> select([b], b.id)
      |> distinct(true)

    activity =
      from(s in Source,
        join: sb in "sources_backends",
        on: sb.source_id == s.id,
        where: s.log_events_updated_at >= ago(1, "day"),
        group_by: sb.backend_id,
        select: %{
          backend_id: sb.backend_id,
          last_active_at: max(s.log_events_updated_at)
        }
      )

    from(b in Backend,
      join: a in subquery(activity),
      on: a.backend_id == b.id,
      where: b.id in subquery(backend_ids),
      order_by: [desc: a.last_active_at, asc: b.id]
    )
  end

  defp maybe_filter_ingesting(query, _ingesting?), do: query

  defp maybe_limit(query, limit) when is_pos_integer(limit), do: limit(query, ^limit)
  defp maybe_limit(query, _limit), do: query

  defp distinct_backends(%Ecto.Query{distinct: nil} = query), do: distinct(query, [b], b.id)
  defp distinct_backends(query), do: query

  @doc """
  Lists `Backend`s by user
  """
  @spec list_backends_by_user_id(integer()) :: [Backend.t()]
  def list_backends_by_user_id(id) when is_integer(id) do
    from(b in Backend, where: b.user_id == ^id)
    |> Repo.all()
    |> Enum.map(fn sb -> typecast_config_string_map_to_atom_map(sb) end)
  end

  @doc """
  Lists all backends a user has access to, including where the user is a team member.
  """
  @spec list_backends_by_user_access(User.t(), keyword()) :: [Backend.t()]
  def list_backends_by_user_access(%User{} = user, filters \\ []) when is_list(filters) do
    Backend
    |> Teams.filter_by_user_access(user)
    |> filter_backends(filters)
    |> Repo.all()
    |> Enum.map(fn sb -> typecast_config_string_map_to_atom_map(sb) end)
  end

  @doc """
  Gets a backend that the user has access to.
  Returns the backend if the user owns it or is a team member, otherwise returns nil.
  """
  @spec get_backend_by_user_access(User.t() | TeamUser.t(), integer() | String.t() | keyword()) ::
          Backend.t() | nil
  def get_backend_by_user_access(user_or_team_user, id) when is_integer(id) or is_binary(id) do
    get_backend_by_user_access(user_or_team_user, id: id)
  end

  def get_backend_by_user_access(user_or_team_user, filters) when is_list(filters) do
    Backend
    |> Teams.filter_by_user_access(user_or_team_user)
    |> where([backend], ^filters)
    |> Repo.one()
    |> typecast_config_string_map_to_atom_map()
  end

  @doc """
  Preload rules for a given backend
  """
  @spec preload_rules(one_or_list_or_nil()) :: one_or_list_or_nil()
  def preload_rules(backends) do
    Repo.preload(backends, rules: [:source])
  end

  @doc """
  Preload alerts key for a given backend
  """
  @spec preload_alerts(one_or_list_or_nil()) :: one_or_list_or_nil()
  def preload_alerts(backends) do
    Repo.preload(backends, [:alert_queries])
  end

  @doc """
  Preload sources for a given backend or list of backends
  """
  @spec preload_sources(one_or_list_or_nil()) :: one_or_list_or_nil()
  def preload_sources(backend_or_backends) do
    Repo.preload(backend_or_backends, :sources)
  end

  @doc """
  Creates a Backend for a given source.
  """
  @spec create_backend(User.t(), map()) :: {:ok, Backend.t()} | {:error, Changeset.t()}
  def create_backend(%User{} = user, attrs) do
    backend =
      user
      |> Ecto.build_assoc(:backends)
      |> Backend.changeset(attrs)
      |> Repo.insert()

    with {:ok, updated} <- backend do
      backend = Repo.preload(updated, :sources)
      Enum.each(backend.sources, &restart_source_sup(&1))

      if updated.default_ingest?, do: default_ingest_flag_changed(updated)

      maybe_start_consolidated_pipeline(updated)

      {:ok, typecast_config_string_map_to_atom_map(updated)}
    end
  end

  def get_default_backend(%User{} = user) do
    if SingleTenant.single_tenant?() and SingleTenant.postgres_backend?() do
      opts = SingleTenant.postgres_backend_adapter_opts()

      %Backend{
        type: :postgres,
        config: Map.new(opts),
        user_id: user.id,
        name: "Default PostgreSQL backend"
      }
    else
      project_id = user.bigquery_project_id || User.bq_project_id()
      dataset_id = user.bigquery_dataset_id || User.generate_bq_dataset_id(user.id)

      %Backend{
        type: :bigquery,
        config: %{
          project_id: project_id,
          dataset_id: dataset_id
        },
        user_id: user.id,
        name: "Default BigQuery backend"
      }
    end
  end

  @doc """
  Returns the backends flagged as the account's default ingest destinations, ordered by id.
  A unique index guarantees at most one per backend type, and only backends that are
  `Adaptor.default_ingest_capable?/1` are returned.
  """
  @spec list_default_ingest_backends(pos_integer()) :: [Backend.t()]
  def list_default_ingest_backends(user_id) when is_pos_integer(user_id) do
    from(b in Backend,
      where: b.user_id == ^user_id and b.default_ingest? == true,
      order_by: [asc: b.id]
    )
    |> Repo.all()
    |> Enum.filter(&Adaptor.default_ingest_capable?/1)
    |> Enum.map(&typecast_config_string_map_to_atom_map/1)
  end

  @doc """
  Backends a source dispatches to: its explicit links plus the account default ingest
  backends when the source participates in default ingest. Participation is read from the
  cached source, falling back to the given struct when it is not cached, so long-lived
  holders of a source struct follow opt in and opt out changes.
  """
  @spec list_dispatch_backends(Source.t()) :: [Backend.t()]
  def list_dispatch_backends(%Source{id: source_id, user_id: user_id} = source) do
    explicit = __MODULE__.Cache.list_backends(source_id: source_id)

    source
    |> cached_or_given_source()
    |> default_ingest_backends_for(user_id)
    |> Enum.concat(explicit)
    |> Enum.uniq_by(& &1.id)
  end

  defp default_ingest_backends_for(%Source{default_ingest_backend_enabled?: true}, user_id),
    do: __MODULE__.Cache.list_default_ingest_backends(user_id)

  defp default_ingest_backends_for(_source, _user_id), do: []

  defp cached_or_given_source(%Source{id: source_id} = source) do
    case Sources.Cache.get_by_id(source_id) do
      %Source{} = cached -> cached
      nil -> source
    end
  end

  @doc """
  Refreshes this node's cached account default ingest backend lookup from the primary
  database, whether or not it was cached. A missing entry is filled, so a WAL bust that just
  deleted it is not followed by a read from a lagging replica.

  The refresh is queued on this node's `DefaultIngestRefresher` partition for the user and
  this call waits for it to run, so refreshes for a user run one at a time in arrival order
  and each reads the primary when it runs. The refresh attempts to start the consolidated
  pipeline of each default on this node, then caches the lookup regardless of whether the
  start succeeded; events dispatched to a default whose pipeline is not running buffer in
  its consolidated queue until `ConsolidatedSupWorker` starts the pipeline.

  Because a refresh can start a pipeline after the read, a refresh that read a default just
  before it was disabled can start that pipeline again after the disable stopped it; the
  orphaned pipeline is stopped by `ConsolidatedSupWorker` on its next check.

  Returns `{:error, reason}` and leaves the cache untouched when the primary read fails with
  a database connection or Postgrex error.

  Multicast to every node when a backend's `default_ingest?` flag changes or a default
  backend is deleted.
  """
  @spec refresh_default_ingest_backends_cache(pos_integer()) :: :ok | {:error, Exception.t()}
  def refresh_default_ingest_backends_cache(user_id) when is_pos_integer(user_id),
    do: DefaultIngestRefresher.refresh(user_id)

  @doc """
  Returns the user ids whose cached account default ingest lookup on this node contains
  any of the given backend ids.
  """
  @spec cached_default_ingest_user_ids([pos_integer()]) :: [pos_integer()]
  def cached_default_ingest_user_ids([]), do: []

  def cached_default_ingest_user_ids(backend_ids) when is_list(backend_ids) do
    for backend_id <- backend_ids,
        {:list_default_ingest_backends, [user_id]} <-
          __MODULE__.Cache.keys_to_bust(id: backend_id),
        uniq: true,
        do: user_id
  end

  @doc """
  Updates the config of a Backend. Returns a changeset error if the backend no longer exists.
  """
  @spec update_backend(Backend.t(), map()) :: {:ok, Backend.t()} | {:error, Changeset.t()}
  def update_backend(%Backend{} = backend, attrs) do
    case update_backend_config(backend, attrs, []) do
      {:error, :not_found} ->
        changeset =
          backend
          |> Changeset.change()
          |> Changeset.add_error(:base, "Backend no longer exists")

        {:error, changeset}

      result ->
        result
    end
  end

  @doc """
  Persists backend query-class policy. Successful saves do not guarantee
  immediate cluster-wide application; metadata cache propagation is eventual.
  """
  @spec configure_query_class_settings(User.t(), Backend.t(), map()) ::
          {:ok, Backend.t()} | {:error, term()}
  def configure_query_class_settings(%User{id: user_id}, %Backend{} = backend, settings) do
    with %User{admin: true} <- Repo.get(User, user_id) do
      update_backend_config(backend, %{config: %{query_class_settings: settings}},
        allow_query_class_settings: true,
        query_policy_only: true
      )
    else
      nil -> {:error, :not_found}
      %User{} -> {:error, :forbidden}
    end
  end

  @spec update_backend_config(Backend.t(), map(), keyword()) ::
          {:ok, Backend.t()} | {:error, Changeset.t() | :not_found | :not_clickhouse_backend}
  defp update_backend_config(%Backend{} = backend, attrs, opts) do
    default_ingest_modified? =
      Map.has_key?(attrs, "default_ingest?") or Map.has_key?(attrs, :default_ingest?)

    config_modified? = Map.has_key?(attrs, "config") or Map.has_key?(attrs, :config)

    result =
      Repo.transact(fn ->
        with {:ok, current} <- lock_backend(backend.id),
             :ok <- validate_query_policy_backend(current, opts) do
          persist_backend_config(current, attrs, opts)
        end
      end)

    case result do
      {:ok, {backend, updated, enabled_only?}} ->
        updated = preload_sources(updated)

        if backend.default_ingest? != updated.default_ingest? do
          default_ingest_flag_changed(updated)
        end

        if Keyword.get(opts, :query_policy_only, false) do
          # Class policy intentionally uses eventually consistent backend metadata.
          # These evictions do not fence in-flight fills or guarantee immediate propagation.
          keys = [{__MODULE__, updated.id}]
          Logflare.ContextCache.bust_keys(keys)
          Cluster.Utils.rpc_multicast(Logflare.ContextCache, :bust_keys, [keys])
        else
          if not enabled_only? do
            Enum.each(updated.sources, &restart_source_sup(&1))
            apply_consolidated_pipeline_change(backend, updated, default_ingest_modified?)
          end

          notify_backend_config_change(updated, config_modified?)
        end

        {:ok, typecast_config_string_map_to_atom_map(updated)}

      error ->
        error
    end
  end

  @spec lock_backend(integer()) :: {:ok, Backend.t()} | {:error, :not_found}
  defp lock_backend(id) do
    case Repo.one(from(b in Backend, where: b.id == ^id, lock: "FOR UPDATE")) do
      nil -> {:error, :not_found}
      backend -> {:ok, typecast_config_string_map_to_atom_map(backend)}
    end
  end

  @spec persist_backend_config(Backend.t(), map(), keyword()) ::
          {:ok, {Backend.t(), Backend.t(), boolean()}} | {:error, Changeset.t()}
  defp persist_backend_config(current, attrs, opts) do
    alerts_modified? = Map.has_key?(attrs, :alert_queries)
    current = if alerts_modified?, do: Repo.preload(current, :alert_queries), else: current

    changeset =
      current
      |> Backend.changeset(attrs, opts)
      |> maybe_put_backend_alerts(attrs, alerts_modified?)

    enabled_only? =
      Map.has_key?(changeset.changes, :enabled) and map_size(changeset.changes) == 1 and
        not Map.has_key?(attrs, :default_ingest?) and not Map.has_key?(attrs, "default_ingest?")

    with {:ok, updated} <- Repo.update(changeset), do: {:ok, {current, updated, enabled_only?}}
  end

  @spec maybe_put_backend_alerts(Changeset.t(), map(), boolean()) :: Changeset.t()
  defp maybe_put_backend_alerts(changeset, attrs, true),
    do: Changeset.put_assoc(changeset, :alert_queries, Map.get(attrs, :alert_queries))

  defp maybe_put_backend_alerts(changeset, _attrs, false), do: changeset

  @spec notify_backend_config_change(Backend.t(), boolean()) :: term()
  defp notify_backend_config_change(_backend, false), do: :ok

  defp notify_backend_config_change(backend, true),
    do: Adaptor.on_backend_config_changed(backend)

  @spec validate_query_policy_backend(Backend.t(), Keyword.t()) ::
          :ok | {:error, :not_clickhouse_backend}
  defp validate_query_policy_backend(%Backend{type: :clickhouse}, _opts), do: :ok

  defp validate_query_policy_backend(_backend, opts) do
    if Keyword.get(opts, :query_policy_only, false),
      do: {:error, :not_clickhouse_backend},
      else: :ok
  end

  @spec default_ingest_flag_changed(Backend.t()) :: :ok
  defp default_ingest_flag_changed(%Backend{user_id: user_id}) do
    Cluster.Utils.rpc_multicast(__MODULE__, :refresh_default_ingest_backends_cache, [user_id])
  end

  @doc """
  Updates the backends of a source wholly. Does not work on partial data, all backends must be provided.
  """
  @spec update_source_backends(Source.t(), [Backend.t()]) ::
          {:ok, Source.t()} | {:error, Changeset.t()}
  def update_source_backends(%Source{} = source, backends) do
    source_with_backends = Sources.preload_backends(source)

    changeset =
      source_with_backends
      |> Changeset.change()
      |> Changeset.put_assoc(:backends, backends)

    with {:ok, _} = result <- Repo.update(changeset) do
      backend_ids = Enum.map(backends, & &1.id)

      cache_keys =
        [{Logflare.Sources, source.id}] ++ Enum.map(backend_ids, &{Logflare.Backends, &1})

      ContextCache.bust_keys(cache_keys)
      clear_list_backends_cache(source.id)

      if source_sup_registered?(source) do
        sync_backend_children(source, source_with_backends.backends, backends)
      else
        :ok
      end

      result
    end
  end

  @spec sync_backend_children(
          source :: Source.t(),
          previous_backends :: [Backend.t()],
          current_backends :: [Backend.t()]
        ) :: :ok
  defp sync_backend_children(source, previous_backends, current_backends) do
    added_backends =
      Enum.filter(current_backends, fn cb ->
        not Enum.any?(previous_backends, &(&1.id == cb.id))
      end)

    removed_backends =
      Enum.filter(previous_backends, fn pb ->
        not Enum.any?(current_backends, &(&1.id == pb.id))
      end)

    for backend <- added_backends do
      SourceSup.start_backend_child(source, backend)
      Cluster.Utils.rpc_multicall(SourceSup, :start_backend_child, [source, backend])
    end

    for backend <- removed_backends do
      SourceSup.stop_backend_child(source, backend)
      Cluster.Utils.rpc_multicall(SourceSup, :stop_backend_child, [source, backend])
    end

    :ok
  end

  @doc """
  Clears cached `list_backends` queries for a specific source.
  """
  @spec clear_list_backends_cache(source_id :: integer()) :: :ok
  def clear_list_backends_cache(source_id) when is_integer(source_id) do
    {:ok, _deleted} = __MODULE__.Cache.delete_keys([{:list_backends, [[source_id: source_id]]}])
    :ok
  end

  # common typecasting from string map to attom for config
  def typecast_config_string_map_to_atom_map(nil), do: nil

  def typecast_config_string_map_to_atom_map(%Backend{type: type} = backend) do
    mod = Backend.adaptor_mapping()[type]

    updated =
      Map.update!(backend, :config_encrypted, fn config ->
        (config || %{})
        |> mod.cast_config()
        |> Changeset.apply_changes()
      end)

    updated
    |> Map.put(:config, updated.config_encrypted)
    |> Map.put(:consolidated_ingest?, Adaptor.consolidated_ingest?(backend))
  end

  @doc """
  Retrieves a Backend by id.
  """
  @spec get_backend(integer()) :: Backend.t() | nil
  def get_backend(id) do
    backend = Repo.get(Backend, id)

    typecast_config_string_map_to_atom_map(backend)
  end

  @doc """
  Retrieves a Backend by id.
  """
  @spec fetch_backend_by(keyword()) :: {:ok, Backend.t()} | {:error, :not_found}
  def fetch_backend_by(kw) do
    backend =
      Repo.get_by(Backend, kw)
      |> typecast_config_string_map_to_atom_map()

    if backend do
      {:ok, backend}
    else
      {:error, :not_found}
    end
  end

  @doc """
  Deletes a Backend
  """
  @spec delete_backend(Backend.t()) :: {:ok, Backend.t()}
  def delete_backend(%Backend{} = backend) do
    result =
      backend
      |> Changeset.change()
      |> Changeset.foreign_key_constraint(:sources,
        name: "sources_backends_backend_id_fkey",
        message: "There are still sources connected to this backend"
      )
      |> Repo.delete()

    with {:ok, deleted} <- result do
      if deleted.default_ingest?, do: default_ingest_flag_changed(deleted)

      maybe_stop_consolidated_pipeline(deleted)
      Adaptor.on_backend_deleted(deleted)
      {:ok, deleted}
    end
  end

  @doc """
  Tests the connection for a given backend.
  """
  @spec test_connection(Backend.t()) :: :ok | {:error, :not_implemented} | {:error, atom()}
  def test_connection(%Backend{} = backend) do
    adaptor = Adaptor.get_adaptor(backend)
    adaptor.test_connection(backend)
  end

  @doc """
  Syncs backend across all cluster nodes for v2 pipeline sources explicitly linked to it.

  This ensures that when a backend's configuration changes, the running `SourceSup`
  instances of its explicitly linked sources are updated immediately without waiting for
  the periodic `SourceSupWorker` check. Account default ingest dispatch does not use links
  and is not affected.
  """
  @spec sync_backend_across_cluster(integer()) :: :ok
  def sync_backend_across_cluster(backend_id) when is_integer(backend_id) do
    with %Backend{} = backend <- get_backend(backend_id) do
      sources = Sources.list_sources(backend_id: backend_id)

      if sources != [] do
        Cluster.Utils.rpc_multicast(__MODULE__, :sync_backends_local, [backend, sources])
      end
    end

    :ok
  end

  @doc """
  Syncs a backend for local node for v2 pipeline sources.
  expects the backend and sources to be loaded from the database.
  """
  @spec sync_backends_local(Backend.t(), [Source.t()]) :: :ok
  def sync_backends_local(%Backend{} = backend, sources) do
    for source <- sources do
      SourceSup.start_backend_child(source, backend)
      clear_list_backends_cache(source.id)
    end

    :ok
  end

  @doc """
  Adds log events to the source event buffer.
  The ingestion pipeline then pulls from the buffer and dispatches log events to the correct backends.

  Events are conditionally dispatched to backends based on whether they are registered. If they register for ingestion dispatching, events will get sent to the registered backend.

  For a spoolable event (gated by `allow_spooling` and the global spool
  mode — plus `source.enable_spooling` when the mode is `:both` — see
  `spoolable?/3`), this blocks the
  caller until the event's segment is durable in the buffer, or — in
  blocking mode (`spool_blocking_mode?/0`) — until its batch is actually
  committed. If spool dispatch fails, `:both` mode falls back to normal
  (non-spool) dispatch; `:producer`-only mode has no backend adaptors to
  fall back to, so it fails the request with `{:error, :spool_unavailable}`
  instead.
  """
  @type log_param :: map()
  @spec ingest_logs([log_param()], Source.t()) ::
          {:ok, count :: non_neg_integer()} | {:error, term()}
  @spec ingest_logs([log_param()], Source.t(), Backend.t() | nil) ::
          {:ok, count :: non_neg_integer()} | {:error, term()}
  @spec ingest_logs([log_param()], Source.t(), Backend.t() | nil, boolean()) ::
          {:ok, count :: non_neg_integer()} | {:error, term()}
  def ingest_logs(event_params, source, backend \\ nil, allow_spooling \\ false)

  def ingest_logs(_params, _source, %Backend{enabled: false}, _spooling), do: {:ok, 0}

  def ingest_logs(event_params, source, backend, allow_spooling) do
    with :ok <- start_for_ingest(source) do
      do_ingest_logs(event_params, source, backend, allow_spooling)
    end
  end

  @doc """
  Makes sure that the SourceSup of the source is up before an ingest writes events.

  Returns `{:error, :source_not_found}` for a deleted source and `{:error, :source_unavailable}`
  for any other failed start. The caller must not write the events then.
  """
  @spec start_for_ingest(Source.t()) :: :ok | {:error, :source_not_found | :source_unavailable}
  def start_for_ingest(source) do
    case ensure_source_sup_started(source) do
      :ok -> :ok
      {:error, :not_found} -> {:error, :source_not_found}
      {:error, _reason} -> {:error, :source_unavailable}
    end
  end

  defp do_ingest_logs(event_params, source, backend, allow_spooling) do
    {log_events, errors} = split_valid_events(source, event_params)
    count = Enum.count(log_events)
    increment_counters(source, count)

    with :ok <- dispatch_logs(source, backend, log_events, allow_spooling) do
      if Enum.empty?(errors), do: {:ok, count}, else: {:error, errors}
    end
  end

  defp dispatch_logs(source, backend, log_events, allow_spooling) do
    if spoolable?(log_events, source, allow_spooling) do
      case dispatch_to_spool_producer(log_events) do
        :ok -> :ok
        {:error, reason} -> handle_spool_dispatch_error(source, backend, log_events, reason)
      end
    else
      dispatch_to_backend_path(source, backend, log_events)
    end
  end

  defp handle_spool_dispatch_error(source, backend, log_events, reason) do
    Logger.error("backends: spool dispatch failed for source #{source.token}: #{inspect(reason)}")

    if spool_mode() == :both do
      dispatch_to_backend_path(source, backend, log_events)
    else
      {:error, :spool_unavailable}
    end
  end

  defp dispatch_to_backend_path(source, backend, log_events) do
    maybe_broadcast_and_route(source, log_events)
    dispatch_to_backends(source, backend, log_events)
    :ok
  end

  @default_spool_append_timeout 15_000

  # No source/partition affinity needed — any partition will do, so a fresh
  # unique term per call spreads load across them the same way random
  # selection did.
  @spec dispatch_to_spool_producer([LogEvent.t()]) :: :ok | {:error, term()}
  defp dispatch_to_spool_producer([]), do: :ok

  defp dispatch_to_spool_producer(log_events) do
    payload = SpoolEncoder.encode_raw_chunk(log_events)
    partition_key = :erlang.unique_integer()

    result =
      case {spool_buffer(), spool_blocking_mode?()} do
        {:mem, false} ->
          DurableBuffer.append_async(SpoolDurableBufferSup.name(), partition_key, payload)

        _ ->
          # WAL mode always blocks on the local fsync tier (that IS its
          # durability guarantee — there's no non-durable ack to skip to);
          # mem mode blocks on the real upload only when spool_blocking_mode?.
          DurableBuffer.append(
            SpoolDurableBufferSup.name(),
            partition_key,
            payload,
            @default_spool_append_timeout
          )
      end

    case result do
      :ok -> :ok
      {:ok, _offset} -> :ok
      {:error, reason} -> {:error, reason}
    end
  rescue
    # The buffer was never started (fresh boot, spool disabled) —
    # DurableBuffer.config/1's :persistent_term lookup has nothing to find.
    ArgumentError -> {:error, :no_spool_partition_available}
  catch
    # The buffer's config is registered but its partition process is gone
    # (subtree crashed and is mid-restart) — the registry lookup resolves
    # to a via-tuple with no live process behind it.
    :exit, _reason -> {:error, :no_spool_partition_available}
  end

  @spec spoolable?([LogEvent.t()], Source.t(), boolean()) :: boolean()
  defp spoolable?(log_events, source, allow_spooling) do
    allow_spooling and
      case spool_mode() do
        :producer ->
          true

        :both ->
          spool_write_healthy?() and (force_spooling?() or source.enable_spooling) and
            Enum.all?(log_events, &(&1.via_rule_id == nil))

        _ ->
          false
      end
  end

  @doc """
  Dispatches events from the spool consumer directly to backends, bypassing the spool producer path.
  Use this in the consumer pipeline to avoid re-routing events back to the spool in `:both` mode.

  `handle` is the spool queue message (SQS receipt handle / PubSub ack id)
  these records were decoded from, or `nil` if they didn't come from the
  spool. Stamped onto each event as `spool_handle` so `IngestEventQueue` can
  track, per handle, how many pipeline completions are still outstanding
  before `Logflare.Backends.Spool.SpoolAck` acks it — see that module's
  moduledoc.
  """
  @spec dispatch_from_spool([map()], Source.t(), term()) ::
          {:ok, non_neg_integer()} | {:error, :source_not_found | :source_unavailable}
  def dispatch_from_spool(spool_records, source, handle \\ nil) do
    with :ok <- start_for_ingest(source) do
      do_dispatch_from_spool(spool_records, source, handle)
    end
  end

  defp do_dispatch_from_spool(spool_records, source, handle) do
    log_events =
      Enum.map(spool_records, fn record ->
        LogEvent.make_from_spool(record, source, handle)
      end)

    count = length(log_events)

    :telemetry.execute(
      [:logflare, :backends, :spool_consumer, :dispatch],
      %{count: count},
      %{}
    )

    maybe_broadcast_and_route(source, log_events)
    dispatch_to_backends(source, nil, log_events)
    {:ok, count}
  end

  defp split_valid_events(source, event_params) do
    max_allowed = System.system_time(:microsecond) + @max_future_event_us

    {events, errors, total, tally} =
      for param <- event_params, reduce: {[], [], 0, %{}} do
        {events, errors, total, tally} ->
          case param_to_log_event(param, source) do
            %{drop: true} ->
              {events, errors, total + 1, bump(tally, :drop_lql)}

            %{pipeline_error: %_{message: message}, valid: false} ->
              {events, [message | errors], total + 1, bump(tally, :rejected)}

            %{body: %{"timestamp" => timestamp}} when timestamp > max_allowed ->
              {events, errors, total + 1, bump(tally, :drop_future)}

            le ->
              {[LogEvent.cache_sizes(le) | events], errors, total + 1, tally}
          end
      end

    emit_ingest_telemetry(tally, source)

    dropped_future = Map.get(tally, :drop_future, 0)

    if dropped_future > 0 do
      Logger.warning(
        "Dropping #{dropped_future} of #{total} event(s): timestamps more than 1h in the future",
        source_id: source.token,
        future_events_dropped: dropped_future,
        total_event_count: total
      )
    end

    {events, errors}
  end

  @spec param_to_log_event(LogEvent.t() | map(), Source.t()) :: LogEvent.t()
  defp param_to_log_event(%LogEvent{source_id: nil} = le, source) do
    %{le | source_id: source.id}
    |> maybe_mark_le_dropped_by_lql(source)
    |> LogEvent.apply_custom_event_message(source)
  end

  defp param_to_log_event(%LogEvent{} = le, source) do
    le
    |> maybe_mark_le_dropped_by_lql(source)
    |> LogEvent.apply_custom_event_message(source)
  end

  defp param_to_log_event(param, source) do
    LogEvent.make(param, %{source: source})
    |> maybe_mark_le_dropped_by_lql(source)
    |> LogEvent.apply_custom_event_message(source)
  end

  defp maybe_mark_le_dropped_by_lql(%LogEvent{} = le, %Source{drop_lql_string: nil}), do: le
  defp maybe_mark_le_dropped_by_lql(%LogEvent{} = le, %Source{drop_lql_string: ""}), do: le
  defp maybe_mark_le_dropped_by_lql(%LogEvent{} = le, %Source{drop_lql_filters: []}), do: le

  defp maybe_mark_le_dropped_by_lql(%LogEvent{} = le, %Source{drop_lql_filters: filters}) do
    if SourceRouter.Sequential.route_with_lql_rules?(le, %Rule{lql_filters: filters}) do
      %{le | drop: true}
    else
      le
    end
  end

  defp increment_counters(source, count) do
    Sources.Counters.increment(source.token, count)
    SystemMetrics.AllLogsLogged.increment(:total_logs_logged, count)
    :ok
  end

  @spec spool_producer_mode?() :: boolean()
  def spool_producer_mode?, do: spool_mode() in [:producer, :both]

  # :wal mode writes to both tiers, so both must be healthy — a WAL that's
  # happily absorbing writes locally while GCS stays down still means the
  # spool isn't actually getting data to its durable home. :mem mode has no
  # local disk tier at all, so only :upload applies there.
  defp spool_write_healthy? do
    case spool_buffer() do
      :wal -> SpoolHealth.healthy?(:disk) and SpoolHealth.healthy?(:upload)
      :mem -> SpoolHealth.healthy?(:upload)
    end
  end

  @spec spool_consumer_mode?() :: boolean()
  def spool_consumer_mode?, do: spool_mode() in [:consumer, :both]

  @doc """
  Whether spool health should gate this node's own healthcheck. Only
  `:producer`-only mode gates on it — that's the only mode where a failing
  spool has no working fallback (see `ingest_logs/4`); `:both` still falls
  back to direct backend dispatch on spool failure, so it shouldn't be
  pulled out of rotation for it.
  """
  @spec spool_healthcheck_ok?() :: boolean()
  def spool_healthcheck_ok? do
    spool_mode() != :producer or spool_write_healthy?()
  end

  @doc """
  Which backend the node's spool `DurableBuffer` instance commits
  through — `:wal` (`Backends.RotatingWal`, local-disk-durable, wrapping
  `Backends.Cloud`, default) or `:mem` (`Backends.Cloud`, straight to
  cloud storage, never durable locally at all). Set via
  `SPOOL_BUFFER`/`:logflare, :spool, :buffer`.
  """
  @spec spool_buffer() :: :wal | :mem
  def spool_buffer,
    do: :logflare |> Application.get_env(:spool, []) |> Keyword.get(:buffer, :wal)

  @doc """
  Whether a spoolable event should block until its batch is actually
  committed, rather than just until it's durable in the buffer. Only
  meaningful in `:mem` buffer mode — `:wal` mode always blocks on the
  local fsync tier, since that's the only durability guarantee it has to
  give. Set via `SPOOL_BLOCKING`/`:logflare, :spool, :blocking` (default
  off).
  """
  @spec spool_blocking_mode?() :: boolean()
  def spool_blocking_mode?,
    do: :logflare |> Application.get_env(:spool, []) |> Keyword.get(:blocking, false)

  defp spool_mode,
    do: :logflare |> Application.get_env(:spool, []) |> Keyword.get(:mode, :disable)

  defp force_spooling?,
    do: :logflare |> Application.get_env(:spool, []) |> Keyword.get(:force_spooling, false)

  @broadcast_rate_ceiling 2

  defp maybe_broadcast_and_route(source, log_events) do
    if log_events != [] do
      RateSampler.bump(source.token, length(log_events))

      if RateSampler.rate(source.token) < @broadcast_rate_ceiling do
        Source.ChannelTopics.broadcast_new(log_events)
      end
    end

    SourceRouter.route_to_sinks_and_ingest(log_events, source)
  end

  # send to a specific backend
  defp dispatch_to_backends(source, %Backend{consolidated_ingest?: true} = backend, log_events) do
    telemetry_metadata = %{backend_type: backend.type}

    :telemetry.span([:logflare, :backends, :ingest, :dispatch], telemetry_metadata, fn ->
      log_events = maybe_pre_ingest(source, backend, log_events)
      IngestEventQueue.add_to_table({:consolidated, backend.id}, log_events)

      :telemetry.execute(
        [:logflare, :backends, :ingest, :dispatch],
        %{count: length(log_events)},
        %{backend_type: backend.type}
      )

      {:ok, telemetry_metadata}
    end)
  end

  defp dispatch_to_backends(source, %Backend{} = backend, log_events) do
    telemetry_metadata = %{backend_type: backend.type}

    :telemetry.span([:logflare, :backends, :ingest, :dispatch], telemetry_metadata, fn ->
      log_events = maybe_pre_ingest(source, backend, log_events)

      queue_key =
        if backend.consolidated_ingest?,
          do: {:consolidated, backend.id},
          else: {source.id, backend.id}

      IngestEventQueue.add_to_table(queue_key, log_events)

      :telemetry.execute(
        [:logflare, :backends, :ingest, :dispatch],
        %{count: length(log_events)},
        %{backend_type: backend.type}
      )

      {:ok, telemetry_metadata}
    end)
  end

  defp dispatch_to_backends(source, nil, log_events) do
    backends = list_dispatch_backends(source)

    for backend <- [nil | backends] do
      dispatch_to_default_backend(source, backend, log_events)
    end
  end

  @spec dispatch_to_default_backend(
          source :: Source.t(),
          backend :: Backend.t() | nil,
          log_events :: [LogEvent.t()]
        ) :: any()
  defp dispatch_to_default_backend(_source, %Backend{enabled: false}, _log_events), do: :ok

  defp dispatch_to_default_backend(source, backend, log_events) do
    {queue_key, backend_type} =
      case backend do
        nil ->
          {{source.id, nil}, SingleTenant.backend_type()}

        %Backend{consolidated_ingest?: true} ->
          {{:consolidated, backend.id}, backend.type}

        %Backend{} ->
          {{source.id, backend.id}, backend.type}
      end

    telemetry_metadata = %{backend_type: backend_type}

    :telemetry.span([:logflare, :backends, :ingest, :dispatch], telemetry_metadata, fn ->
      log_events =
        if backend, do: maybe_pre_ingest(source, backend, log_events), else: log_events

      IngestEventQueue.add_to_table(queue_key, log_events)

      :telemetry.execute(
        [:logflare, :backends, :ingest, :dispatch],
        %{count: length(log_events)},
        %{backend_type: backend_type}
      )

      {:ok, telemetry_metadata}
    end)
  end

  defp maybe_pre_ingest(source, backend, events) do
    mod = Adaptor.get_adaptor(backend)

    if Adaptor.exports?(mod, :pre_ingest, 3) do
      mod.pre_ingest(source, backend, events)
    else
      events
    end
  end

  @doc """
  Registers a unique source-related process on the source registry. Unique.
  For internal use only, should not be called outside of the `Logflare` namespace.

  ### Example
  iex> Backends.via_source(source,  __MODULE__, backend.id)
  iex> Backends.via_source(source, :buffer)
  """
  @spec via_source(Source.t() | non_neg_integer(), term()) :: {:via, module(), term()}
  @spec via_source(
          Source.t() | non_neg_integer(),
          module(),
          Backend.t() | non_neg_integer() | nil
        ) ::
          {:via, module(), term()}
  def via_source(%Source{id: sid}, mod, backend), do: via_source(sid, mod, backend)
  def via_source(source, mod, %Backend{id: bid}), do: via_source(source, mod, bid)
  def via_source(source_id, mod, backend_id), do: via_source(source_id, {mod, backend_id})

  def via_source(%Source{id: id}, process_id), do: via_source(id, process_id)

  def via_source(id, process_id) when is_number(id) do
    {:via, Registry, {SourceRegistry, {id, process_id}}}
  end

  @doc """
  Registers a unique backend-related process on the backend registry.
  """
  @spec via_backend(Backend.t() | non_neg_integer(), module()) :: {:via, module(), term()}
  def via_backend(%Backend{id: id}, mod), do: via_backend(id, mod)

  def via_backend(backend_id, mod) when is_number(backend_id) do
    {:via, Registry, {BackendRegistry, {mod, backend_id}}}
  end

  @doc """
  Like `via_backend/2` but adds a `label` dimension to the registry key. `nil` reuses the
  `via_backend/2` key, so legacy/unlabeled callers are unchanged.
  """
  @spec via_backend(Backend.t() | non_neg_integer(), module(), String.t() | nil) ::
          {:via, module(), term()}
  def via_backend(%Backend{id: id}, mod, label), do: via_backend(id, mod, label)

  def via_backend(backend_id, mod, nil), do: via_backend(backend_id, mod)

  def via_backend(backend_id, mod, label) when is_pos_integer(backend_id) do
    {:via, Registry, {BackendRegistry, {mod, backend_id, label}}}
  end

  @doc """
  drop in replacement for Source.Supervisor.lookup
  """
  def lookup(module, source_token) when is_atom(source_token) do
    source = Sources.Cache.get_source_by_token(source_token)
    lookup(module, source)
  end

  def lookup(module, %Source{} = source) do
    via_source(source, module)
    |> GenServer.whereis()
    |> case do
      pid when is_pid(pid) -> {:ok, pid}
      _ -> {:error, :not_started}
    end
  end

  @doc """
  Checks if the SourceSup for a given source is up and all its initial children are started.

  A `SourceSup` that is registered but still in its start returns `false`. See
  `Logflare.Backends.SourceSup.mark_ready/1`.
  """
  @spec source_sup_started?(Source.t() | non_neg_integer()) :: boolean()
  def source_sup_started?(%Source{id: id}), do: source_sup_started?(id)

  def source_sup_started?(id) when is_number(id) do
    match?([{_pid, :ready}], Registry.lookup(SourceRegistry, {id, SourceSup}))
  end

  @doc """
  Checks if the SourceSup for a given source is registered. It can still be in its start.

  Backend reconciliation uses this check. A child start or stop on a `SourceSup` in its start
  waits until its `init/1` returns. Thus a backend change during the start still reaches the
  `SourceSup`. Ingest must use `source_sup_started?/1`.
  """
  @spec source_sup_registered?(Source.t() | non_neg_integer()) :: boolean()
  def source_sup_registered?(%Source{id: id}), do: source_sup_registered?(id)

  def source_sup_registered?(id) when is_number(id) do
    Registry.lookup(SourceRegistry, {id, SourceSup}) != []
  end

  @doc """
  Starts the SourceSup of a source.

  Returns `{:error, :already_started}` when the SourceSup is already up.

  The start has two steps: `SourceSup.prefetch/1` warms the caches, then the `SourcesSup`
  partition starts the `SourceSup`. The caller waits for both steps for at most the
  `:source_sup_start_timeout` application environment value. The default is 30 seconds. After
  that time, the caller gets `{:error, :start_timeout}`. A blocked cache or partition can delay
  ingest, but it can not hold the callers forever. The partition still completes the start after
  the timeout.

  A failed start logs its reason, except for `:already_started` and `:not_found`.
  """
  @spec start_source_sup(Source.t()) ::
          :ok | {:error, :already_started | :not_found | :start_timeout | term()}
  def start_source_sup(%Source{} = source), do: do_start_source_sup(source.id, fn -> source end)

  @spec do_start_source_sup(pos_integer(), (-> Source.t() | nil)) ::
          :ok | {:error, :already_started | :not_found | :start_timeout | term()}
  defp do_start_source_sup(source_id, load_source) do
    timeout =
      Application.get_env(:logflare, :source_sup_start_timeout, @default_source_sup_start_timeout)

    task =
      Task.Supervisor.async_nolink(
        {:via, PartitionSupervisor, {Logflare.TaskSupervisors, source_id}},
        fn -> start_loaded_source_sup(load_source.()) end
      )

    case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} ->
        result
        |> handle_start_child_result()
        |> log_start_failure(source_id)

      {:exit, reason} ->
        log_start_failure({:error, reason}, source_id)

      nil ->
        Logger.warning("SourceSup start timed out after #{timeout}ms", source_id: source_id)
        {:error, :start_timeout}
    end
  end

  defp start_loaded_source_sup(%Source{} = source) do
    if not source_sup_started?(source), do: SourceSup.prefetch(source)

    DynamicSupervisor.start_child(
      {:via, PartitionSupervisor, {SourcesSup, source.id}},
      SourceSup.child_spec(source)
    )
  end

  defp start_loaded_source_sup(nil), do: :ignore

  defp handle_start_child_result(result) do
    case result do
      {:ok, _pid} ->
        :ok

      :ignore ->
        {:error, :not_found}

      {:error, {:already_started = reason, _pid}} ->
        {:error, reason}

      {:error, {:shutdown, {:failed_to_start_child, _mod, {:already_started = reason, _pid}}}} ->
        {:error, reason}

      {:error, _reason} = error ->
        error
    end
  end

  defp log_start_failure({:error, reason} = error, source_id)
       when reason not in [:already_started, :not_found] do
    Logger.error("SourceSup start failed: #{inspect(reason)}", source_id: source_id)
    error
  end

  defp log_start_failure(result, _source_id), do: result

  @doc """
  Makes sure that the SourceSup of a source is up.

  Every ingest batch calls this function. When the SourceSup is down, the first caller for the
  source starts it. The other callers for that source wait for that start. Each caller gets the
  result of the same start. Thus the `SourcesSup` partition gets one `start_child` call per source.

  The start loads the source by id with `Sources.Cache.get_by_id_or_primary/1`. The lookup, the
  prefetch and the `start_child` call share the deadline of `start_source_sup/1`. So a stalled
  cache or primary read returns `{:error, :start_timeout}` to every caller.

  The function also takes a source id. Then the caller does not need its own lookup, which would
  run outside the deadline.

  A failed start returns `{:error, reason}`. It does not raise.
  """
  @spec ensure_source_sup_started(Source.t() | pos_integer()) ::
          :ok | {:error, :not_found | :start_timeout | term()}
  def ensure_source_sup_started(%Source{id: id}), do: ensure_source_sup_started(id)

  def ensure_source_sup_started(id) when is_integer(id) do
    if source_sup_started?(id) do
      :ok
    else
      SourceSupStarts
      |> Cachex.fetch(id, &start_source_sup_by_id/1)
      |> handle_source_sup_start()
    end
  end

  @spec start_source_sup_by_id(pos_integer()) :: {:ignore, :ok | {:error, term()}}
  defp start_source_sup_by_id(source_id) do
    {:ignore,
     do_start_source_sup(source_id, fn -> Sources.Cache.get_by_id_or_primary(source_id) end)}
  end

  defp handle_source_sup_start({:ignore, :ok}), do: :ok
  defp handle_source_sup_start({:ignore, {:error, :already_started}}), do: :ok
  defp handle_source_sup_start({:ignore, {:error, _reason} = error}), do: error
  defp handle_source_sup_start({:error, _reason} = error), do: error

  @doc """
  Stops a given SourceSup for a source. if not started, it will return an error tuple.
  """
  @spec stop_source_sup(Source.t()) :: :ok | {:error, :not_started}
  def stop_source_sup(%Source{} = source) do
    with [{pid, _}] <- Registry.lookup(SourceRegistry, {source.id, SourceSup}),
         :ok <-
           DynamicSupervisor.terminate_child(
             {:via, PartitionSupervisor, {SourcesSup, source.id}},
             pid
           ) do
      :ok
    else
      _ -> {:error, :not_started}
    end
  end

  @doc """
  Restarts a SourceSup of a given source.
  """
  @spec restart_source_sup(Source.t()) ::
          :ok | {:error, :already_started | :not_started | :not_found | :start_timeout | term()}
  def restart_source_sup(%Source{} = source) do
    with :ok <- stop_source_sup(source),
         :ok <- start_source_sup(source) do
      :ok
    end
  end

  @doc """
  Starts a consolidated pipeline for a backend if it supports consolidated ingestion.
  """
  @spec maybe_start_consolidated_pipeline(Backend.t()) :: :ok
  def maybe_start_consolidated_pipeline(%Backend{} = backend) do
    if Adaptor.consolidated_ingest?(backend) do
      case ConsolidatedSup.start_pipeline(backend) do
        {:ok, _pid} ->
          Logger.info("Started consolidated pipeline", backend_id: backend.id)

        {:error, {:already_started, _pid}} ->
          :ok

        {:error, reason} ->
          Logger.warning("Failed to start consolidated pipeline: #{inspect(reason)}",
            backend_id: backend.id
          )
      end
    end

    :ok
  end

  @spec apply_consolidated_pipeline_change(Backend.t(), Backend.t(), boolean()) :: :ok
  defp apply_consolidated_pipeline_change(backend, updated, default_ingest_modified?) do
    updated = typecast_config_string_map_to_atom_map(updated)
    pipeline_keys = Adaptor.pipeline_config_keys(updated)
    runtime_keys = Adaptor.runtime_config_keys(updated)

    changes =
      backend
      |> typecast_config_string_map_to_atom_map()
      |> changed_config_keys(updated)

    pipeline_changes = Enum.filter(changes, &(&1 in pipeline_keys))
    other_changes = Enum.reject(changes, &(&1 in pipeline_keys or &1 in runtime_keys))

    if default_ingest_modified? or other_changes != [] or changes == [] do
      maybe_restart_consolidated_pipeline(updated)
    end

    if pipeline_changes != [] do
      Cluster.Utils.rpc_multicast(__MODULE__, :replace_pipelines_after_delay, [updated.id])
    end

    :ok
  end

  @doc false
  @spec replace_pipelines_after_delay(pos_integer()) :: :ok | {:error, term()}
  def replace_pipelines_after_delay(backend_id) when is_integer(backend_id) do
    Process.sleep(pipeline_swap_delay_ms())
    replace_pipelines_with_latest(backend_id)
  end

  @doc false
  @spec replace_pipelines_with_latest(integer()) :: :ok | {:error, term()}
  def replace_pipelines_with_latest(backend_id) when is_integer(backend_id) do
    case get_backend(backend_id) do
      nil -> :ok
      backend -> Adaptor.replace_pipelines(backend)
    end
  end

  @doc false
  @spec pipeline_swap_delay_ms() :: non_neg_integer()
  def pipeline_swap_delay_ms, do: :rand.uniform(@max_pipeline_swap_delay_ms + 1) - 1

  @spec changed_config_keys(Backend.t(), Backend.t()) :: [atom()]
  defp changed_config_keys(%Backend{config: before}, %Backend{config: after_update}) do
    (Map.keys(before) ++ Map.keys(after_update))
    |> Enum.uniq()
    |> Enum.reject(&(Map.get(before, &1) == Map.get(after_update, &1)))
  end

  @doc """
  Restarts a consolidated pipeline for a backend if it supports consolidated ingestion.
  """
  @spec maybe_restart_consolidated_pipeline(Backend.t()) :: :ok
  def maybe_restart_consolidated_pipeline(%Backend{} = backend) do
    if Adaptor.consolidated_ingest?(backend) do
      ConsolidatedSup.stop_pipeline(backend)
      maybe_start_consolidated_pipeline(backend)
    end

    :ok
  end

  @doc """
  Stops a consolidated pipeline for a backend if it supports consolidated ingestion.
  """
  @spec maybe_stop_consolidated_pipeline(Backend.t()) :: :ok
  def maybe_stop_consolidated_pipeline(%Backend{} = backend) do
    with true <- Adaptor.consolidated_ingest?(backend),
         :ok <- ConsolidatedSup.stop_pipeline(backend) do
      Logger.info("Stopped consolidated pipeline", backend_id: backend.id)
    end

    :ok
  end

  @doc """
  Uses the buffers cache in `PubSubRates.Cache` to determine if a source's pending buffer
  is full on this node. A buffer is full when every one of its queues is over its limit.

  For sources with `default_ingest_backend_enabled? = true`, returns true if the system
  default backend buffer is full or the buffer of any enabled `default_ingest?` backend among
  `list_dispatch_backends/1` is full. That covers both explicitly linked backends flagged
  `default_ingest?` and the account default ingest backends the source receives without a
  link. A consolidated backend is checked against its shared `{:consolidated, backend_id}`
  buffer and `IngestEventQueue.max_consolidated_queue_size/0`; any other backend against
  its per-source buffer and `max_buffer_queue_len/0`. Disabled backends and backends not
  flagged `default_ingest?` are ignored.

  For other sources, only the system default backend buffer is checked.
  """
  @spec cached_local_pending_buffer_full?(Source.t()) :: boolean()
  def cached_local_pending_buffer_full?(
        %Source{id: source_id, default_ingest_backend_enabled?: true} = source
      ) do
    buffer_full_for_backend?(source_id, nil) or
      source
      |> list_dispatch_backends()
      |> Enum.filter(&(&1.enabled and &1.default_ingest?))
      |> Enum.any?(&buffer_full_for_backend?(source_id, &1))
  end

  def cached_local_pending_buffer_full?(%Source{id: source_id}) do
    buffer_full_for_backend?(source_id, nil)
  end

  @spec buffer_full_for_backend?(non_neg_integer(), Backend.t() | pos_integer() | nil) ::
          boolean()
  defp buffer_full_for_backend?(_source_id, %Backend{consolidated_ingest?: true, id: backend_id}) do
    :consolidated
    |> PubSubRates.Cache.get_local_buffer(backend_id)
    |> all_queues_over?(IngestEventQueue.max_consolidated_queue_size())
  end

  defp buffer_full_for_backend?(source_id, %Backend{id: backend_id}),
    do: buffer_full_for_backend?(source_id, backend_id)

  defp buffer_full_for_backend?(source_id, backend_id) do
    source_id
    |> PubSubRates.Cache.get_local_buffer(backend_id)
    |> all_queues_over?(@max_pending_buffer_len_per_queue)
  end

  defp all_queues_over?(%{queues: [_ | _] = queues}, limit),
    do: Enum.all?(queues, fn {_key, count} -> count > limit end)

  defp all_queues_over?(_buffer, _limit), do: false

  @doc """
  Returns true if ANY of a source's ingest queues on this node currently exceeds its limit:
  the system default queue plus every enabled backend in `list_dispatch_backends/1`, regardless of
  `default_ingest?` flags. A consolidated backend is checked through its shared
  `{:consolidated, backend_id}` queues against
  `IngestEventQueue.max_consolidated_queue_size/0`; any other backend through its
  per-source queues against `max_buffer_queue_len/0`. Unlike
  `cached_local_pending_buffer_full?/1` (scoped to the system default and `default_ingest?`
  backends for the HTTP 429 gate), this reads live `IngestEventQueue` sizes directly rather
  than a separately-cadenced cache, matching `QueueJanitor`'s own `get_table_size/1`-based
  overflow check. When the source is not cached, only its explicitly linked backends are
  checked. Disabled backend pipelines may keep draining queued work; the spool memory
  monitor independently protects node memory.
  """
  @spec any_ingest_queue_over_limit?(pos_integer()) :: boolean()
  def any_ingest_queue_over_limit?(source_id) when is_pos_integer(source_id) do
    queue_over_limit_for_backend?(source_id, nil) or
      source_id
      |> backends_for_queue_limit()
      |> Enum.any?(&queue_over_limit_for_backend?(source_id, &1))
  end

  defp backends_for_queue_limit(source_id) do
    case Sources.Cache.get_by_id(source_id) do
      %Source{} = source -> list_dispatch_backends(source)
      nil -> __MODULE__.Cache.list_backends(source_id: source_id)
    end
  end

  defp queue_over_limit_for_backend?(_source_id, %Backend{enabled: false}), do: false

  defp queue_over_limit_for_backend?(_source_id, %Backend{consolidated_ingest?: true, id: bid}),
    do: any_queue_over?({:consolidated, bid}, IngestEventQueue.max_consolidated_queue_size())

  defp queue_over_limit_for_backend?(source_id, %Backend{id: backend_id}),
    do: queue_over_limit_for_backend?(source_id, backend_id)

  defp queue_over_limit_for_backend?(source_id, backend_id),
    do: any_queue_over?({source_id, backend_id}, max_buffer_queue_len())

  defp any_queue_over?(queues_key, limit) do
    queues_key
    |> IngestEventQueue.list_queues()
    |> Enum.any?(&queue_over_limit?(&1, limit))
  end

  defp queue_over_limit?(table_key, limit) do
    case IngestEventQueue.get_table_size(table_key) do
      size when is_integer(size) -> size > limit
      _ -> false
    end
  end

  @doc """
  Caches total buffer len. Includes ingested events that are awaiting cleanup.
  """
  @spec cache_local_buffer_lens(non_neg_integer() | :consolidated, non_neg_integer() | nil) ::
          {:ok,
           %{
             len: non_neg_integer(),
             queues: [{Logflare.Backends.IngestEventQueue.table_key(), non_neg_integer()}]
           }}
  def cache_local_buffer_lens(source_id, backend_id \\ nil) do
    queues = IngestEventQueue.list_counts({source_id, backend_id})

    len = for({_k, v} <- queues, do: v) |> Enum.sum()

    stats = %{len: len, queues: queues}
    payload = %{Node.self() => stats}
    PubSubRates.Cache.cache_buffers(source_id, backend_id, payload)
    {:ok, stats}
  end

  @doc """
  Get local pending buffer len of a source/backend combination.
  """
  @spec cached_local_pending_buffer_len(Source.t(), Backend.t() | nil) :: map()
  def cached_local_pending_buffer_len(source_id, backend_id \\ nil) when is_integer(source_id) do
    PubSubRates.Cache.get_local_buffer(source_id, backend_id)
  end

  @doc """
  Retrieves cluster-wide pending buffer size stored in cache for a given backend/source combination.
  """
  @spec cached_pending_buffer_len(Source.t(), Backend.t() | nil) :: non_neg_integer()
  def cached_pending_buffer_len(%Source{} = source, backend \\ nil) do
    PubSubRates.Cache.get_cluster_buffers(source.id, Map.get(backend || %{}, :id))
  end

  @doc """
  Lists the latest recent logs of all caches across the cluster.
  Performs a check to ensure that the cache is started. If not started yet globally, it will start the cache locally.
  """
  @spec list_recent_logs(Source.t()) :: [LogEvent.t()]
  def list_recent_logs(%Source{} = source) do
    nodes = Cluster.Utils.node_list_all()

    :erpc.multicall(nodes, __MODULE__, :list_recent_logs_local, [source.id], 5_000)
    |> Enum.map(fn
      {:ok, result} when is_list(result) -> result
      _ -> []
    end)
    |> List.flatten()
    |> Enum.sort_by(& &1.body["timestamp"], &<=/2)
    |> Enum.take(-100)
  end

  def fetch_latest_timestamp(%Source{} = source) do
    Counters.get_source_changed_at_unix_ms(source.token)
  end

  @doc """
  Lists latest recent logs of only the local cache.

  For consolidated backends, returns an empty list since filtering by source
  in a consolidated queue would require scanning all events and is expensive.
  """
  @spec list_recent_logs_local(Source.t() | pos_integer()) :: [LogEvent.t()]
  @spec list_recent_logs_local(Source.t() | pos_integer(), n :: number()) :: [LogEvent.t()]
  @spec list_recent_logs_local(Source.t(), Backend.t()) :: [LogEvent.t()]
  def list_recent_logs_local(source, n \\ 100)

  def list_recent_logs_local(%Source{id: id}, n) when is_number(n),
    do: list_recent_logs_local(id, n)

  def list_recent_logs_local(%Source{} = source, %Backend{} = backend) do
    if Adaptor.consolidated_ingest?(backend) do
      []
    else
      list_recent_logs_local(source)
    end
  end

  def list_recent_logs_local(source_id, n) when is_integer(source_id) and is_number(n) do
    {:ok, events} = IngestEventQueue.fetch_events({source_id, nil}, n)

    events
    |> Enum.sort_by(& &1.body["timestamp"], &<=/2)
    |> Enum.take(-n)
  end

  @doc """
  Pipeline count resolution logic, for DynamicPipeline, shared across BigQuery and ClickHouse.
  """
  @spec handle_resolve_count(
          %{
            pipeline_count: non_neg_integer(),
            max_pipelines: non_neg_integer(),
            last_count_increase: NaiveDateTime.t() | nil,
            last_count_decrease: NaiveDateTime.t() | nil
          },
          [
            {
              {pos_integer(), pos_integer() | nil, reference() | nil},
              non_neg_integer()
            }
          ],
          non_neg_integer()
        ) :: non_neg_integer()
  def handle_resolve_count(state, lens, avg_rate) do
    startup_size =
      Enum.find_value(lens, 0, fn
        {{_sid, _bid, nil}, val} -> val
        _ -> false
      end)

    lens_no_startup =
      Enum.filter(lens, fn
        {{_sid, _bid, nil}, _val} -> false
        _ -> true
      end)

    lens_no_startup_values = Enum.map(lens_no_startup, fn {_, v} -> v end)
    len = Enum.map(lens, fn {_, v} -> v end) |> Enum.sum()

    last_decr = state.last_count_decrease || NaiveDateTime.utc_now()
    sec_since_last_decr = NaiveDateTime.diff(NaiveDateTime.utc_now(), last_decr)

    any_above_threshold? = Enum.any?(lens_no_startup_values, &(&1 >= 500))

    cond do
      # max out pipelines, overflow risk
      startup_size > 0 ->
        state.pipeline_count + ceil(startup_size / 500)

      any_above_threshold? and len > 0 ->
        state.pipeline_count + ceil(len / 500)

      # gradual decrease
      Enum.all?(lens_no_startup_values, &(&1 < 50)) and len < 500 and state.pipeline_count > 1 and
          (sec_since_last_decr > 60 or state.last_count_decrease == nil) ->
        state.pipeline_count - 1

      len == 0 and avg_rate == 0 and
        state.pipeline_count == 1 and
          (sec_since_last_decr > 60 * 5 or state.last_count_decrease == nil) ->
        # scale to zero only if no items for > 5m
        0

      true ->
        state.pipeline_count
    end
  end

  defp bump(tally, reason), do: Map.update(tally, reason, 1, &(&1 + 1))

  defp emit_ingest_telemetry(tally, source) do
    for {reason, count} <- tally do
      :telemetry.execute(
        [:logflare, :logs, :ingest_logs, reason],
        %{count: count},
        %{source_id: source.id, source_token: source.token}
      )
    end
  end
end
