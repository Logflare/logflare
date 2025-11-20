defmodule Logflare.Backends do
  @moduledoc false

  import Ecto.Query
  import Logflare.Utils.Guards

  alias Ecto.Changeset
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Backend
  alias Logflare.Backends.BackendRegistry
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.RecentEventsTouch
  alias Logflare.Backends.SourceRegistry
  alias Logflare.Backends.SourcesSup
  alias Logflare.Backends.SourceSup
  alias Logflare.ContextCache
  alias Logflare.Cluster
  alias Logflare.LogEvent
  alias Logflare.Logs.SourceRouting
  alias Logflare.PubSubRates
  alias Logflare.Repo
  alias Logflare.SingleTenant
  alias Logflare.Sources.Source
  alias Logflare.Sources
  alias Logflare.Sources.Counters
  alias Logflare.SystemMetrics
  alias Logflare.Rules.Rule
  alias Logflare.User

  defdelegate child_spec(arg), to: __MODULE__.Supervisor

  @max_pending_buffer_len_per_queue 15_000

  @doc """
  Retrieves the hardcoded max pending buffer length of an individual queue
  """
  @spec max_buffer_queue_len() :: non_neg_integer()
  def max_buffer_queue_len, do: @max_pending_buffer_len_per_queue

  @doc """
  Lists `Backend`s for a given source.
  """
  @spec list_backends(keyword()) :: [Backend.t()]
  def list_backends(filters) when is_list(filters) do
    filters
    |> Enum.reduce(from(b in Backend), fn
      {:types, types}, q when is_list(types) ->
        where(q, [b], b.type in ^types)

      # filter down to backends of this source
      {:source_id, id}, q ->
        join(q, :inner, [b], s in assoc(b, :sources), on: s.id == ^id)

      # filter down to backends with rules destinations
      {:rules_source_id, source_id}, q ->
        join(q, :inner, [b], r in assoc(b, :rules), on: r.source_id == ^source_id)

      # filter down to backends with sources that have recently ingested.
      # orders by the last active.
      {:ingesting, true}, q ->
        q
        |> join(:inner, [b], s in assoc(b, :sources),
          on: s.log_events_updated_at >= ago(1, "day")
        )
        |> order_by([..., s], {:desc, s.log_events_updated_at})

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

      _, q ->
        q
    end)
    |> Repo.all()
    |> Enum.map(fn sb -> typecast_config_string_map_to_atom_map(sb) end)
  end

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
  Preload rules for a given backend
  """
  @spec preload_rules([Backend.t()]) :: [Backend.t()]
  def preload_rules(backends) do
    Repo.preload(backends, rules: [:source])
  end

  @doc """
  Preload alerts key for a given backend
  """
  @spec preload_alerts([Backend.t()]) :: [Backend.t()]
  def preload_alerts(backends) do
    Repo.preload(backends, [:alert_queries])
  end

  @doc """
  Preload sources for a given backend or list of backends
  """
  @spec preload_sources(Backend.t() | [Backend.t()]) :: Backend.t() | [Backend.t()]
  def preload_sources(backend_or_backends) do
    Repo.preload(backend_or_backends, :sources)
  end

  @doc """
  Creates a Backend for a given source.
  """
  @spec create_backend(map()) :: {:ok, Backend.t()} | {:error, Changeset.t()}
  def create_backend(attrs) do
    backend =
      %Backend{}
      |> Backend.changeset(attrs)
      |> Repo.insert()

    with {:ok, updated} <- backend do
      backend = Repo.preload(updated, :sources)
      Enum.each(backend.sources, &restart_source_sup(&1))

      if updated.default_ingest?, do: sync_backend_across_cluster(updated.id)

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
        name: "Default postgres backend"
      }
    else
      {project_id, dataset_id} =
        if user.bigquery_project_id do
          {user.bigquery_project_id, user.bigquery_dataset_id}
        else
          project_id = User.bq_project_id()
          dataset_id = User.generate_bq_dataset_id(user.id)
          {project_id, dataset_id}
        end

      %Backend{
        type: :bigquery,
        config: %{
          project_id: project_id,
          dataset_id: dataset_id
        },
        user_id: user.id,
        name: "Default bigquery backend"
      }
    end
  end

  @doc """
  Updates the config of a Backend.
  """
  @spec update_backend(Backend.t(), map()) :: {:ok, Backend.t()} | {:error, Changeset.t()}
  def update_backend(%Backend{} = backend, attrs) do
    alerts_modified = Map.has_key?(attrs, :alert_queries)

    default_ingest_modified? =
      Map.has_key?(attrs, "default_ingest?") or Map.has_key?(attrs, :default_ingest?)

    source_id = Map.get(attrs, "source_id") || Map.get(attrs, :source_id)

    changeset =
      backend
      |> Backend.changeset(attrs)
      |> validate_default_ingest_source(source_id)
      |> then(fn changeset ->
        if alerts_modified do
          Changeset.put_assoc(changeset, :alert_queries, Map.get(attrs, :alert_queries))
        else
          changeset
        end
      end)

    case Repo.update(changeset) do
      {:ok, updated} ->
        updated = preload_sources(updated)

        updated =
          if default_ingest_modified? do
            was_enabled? = backend.default_ingest?
            is_enabled? = updated.default_ingest?
            handle_default_ingest_associations(updated, source_id, was_enabled?, is_enabled?)
            sync_backend_across_cluster(updated.id)

            # force refresh after association changes
            Repo.reload!(updated) |> preload_sources()
          else
            updated
          end

        Enum.each(updated.sources, &restart_source_sup(&1))

        {:ok, typecast_config_string_map_to_atom_map(updated)}

      error ->
        error
    end
  end

  @spec validate_default_ingest_source(Changeset.t(), String.t() | integer() | nil) ::
          Changeset.t()
  defp validate_default_ingest_source(%{changes: %{default_ingest?: true}} = changeset, source_id)
       when is_non_empty_binary(source_id) or is_integer(source_id) do
    case Sources.get(source_id) do
      %Source{default_ingest_backend_enabled?: true} ->
        changeset

      %Source{default_ingest_backend_enabled?: false} ->
        Changeset.add_error(
          changeset,
          :default_ingest?,
          "Source must have default ingest backend support enabled"
        )

      nil ->
        Changeset.add_error(
          changeset,
          :default_ingest?,
          "Source not found"
        )
    end
  end

  defp validate_default_ingest_source(
         %Changeset{changes: %{default_ingest?: true}} = changeset,
         _source_id
       ) do
    Changeset.add_error(
      changeset,
      :default_ingest?,
      "Please select a source when enabling default ingest"
    )
  end

  defp validate_default_ingest_source(changeset, _source_id), do: changeset

  @spec handle_default_ingest_associations(
          Backend.t(),
          source_id :: String.t() | integer() | nil,
          was_enabled :: boolean(),
          is_enabled :: boolean()
        ) :: :ok
  defp handle_default_ingest_associations(backend, source_id, _was_enabled, true)
       when is_non_empty_binary(source_id) or is_integer(source_id) do
    source = Sources.get(source_id) |> Sources.preload_backends()

    if not Enum.any?(source.backends, &(&1.id == backend.id)) do
      update_source_backends(source, source.backends ++ [backend])
    end

    :ok
  end

  defp handle_default_ingest_associations(backend, _source_id, true, false) do
    backend_with_sources =
      backend
      |> Repo.reload!()
      |> Repo.preload(:sources)

    Enum.each(backend_with_sources.sources, fn source ->
      source = Sources.preload_backends(source)
      filtered_backends = Enum.reject(source.backends, &(&1.id == backend.id))
      update_source_backends(source, filtered_backends)
    end)

    :ok
  end

  defp handle_default_ingest_associations(_backend, _source_id, _was_enabled, _is_enabled),
    do: :ok

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

      if source_sup_started?(source) do
        previous_backends = source_with_backends.backends
        current_backends = backends

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
      else
        :ok
      end

      result
    end
  end

  @doc """
  Clears cached `list_backends` queries for a specific source.
  """
  @spec clear_list_backends_cache(source_id :: integer()) :: :ok
  def clear_list_backends_cache(source_id) when is_integer(source_id) do
    Cachex.del(__MODULE__.Cache, {:list_backends, [[source_id: source_id]]})
    Cachex.del(__MODULE__.Cache, {:list_backends, [source_id: source_id]})
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

    Map.put(updated, :config, updated.config_encrypted)
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
    backend
    |> Changeset.change()
    |> Changeset.foreign_key_constraint(:sources,
      name: "sources_backends_backend_id_fkey",
      message: "There are still sources connected to this backend"
    )
    |> Repo.delete()
  end

  @doc """
  Syncs backend across all cluster nodes for v2 pipeline sources.

  This ensures that when a backend's configuration changes (e.g., `default_ingest?` flag),
  all running `SourceSup` instances are updated immediately without waiting for the
  periodic `SourceSupWorker` check.
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

  Once this function returns `:ok`, the events get dispatched to respective backend adaptor portions of the pipeline to be further processed.
  """
  @type log_param :: map()
  @spec ingest_logs([log_param()], Source.t()) ::
          {:ok, count :: pos_integer()} | {:error, [term()]}
  @spec ingest_logs([log_param()], Source.t(), Backend.t() | nil) ::
          {:ok, count :: pos_integer()} | {:error, [term()]}
  def ingest_logs(event_params, source, backend \\ nil) do
    ensure_source_sup_started(source)
    {log_events, errors} = split_valid_events(source, event_params)
    count = Enum.count(log_events)
    increment_counters(source, count)
    maybe_broadcast_and_route(source, log_events)
    dispatch_to_backends(source, backend, log_events)
    if Enum.empty?(errors), do: {:ok, count}, else: {:error, errors}
  end

  defp split_valid_events(source, event_params) do
    event_params
    |> Enum.reduce({[], []}, fn param, {events, errors} ->
      param
      |> case do
        %LogEvent{source_id: nil} = le ->
          %{le | source_id: source.id}

        %LogEvent{} = le ->
          le

        param ->
          LogEvent.make(param, %{source: source})
      end
      |> maybe_mark_le_dropped_by_lql(source)
      |> LogEvent.apply_custom_event_message(source)
      |> case do
        %{drop: true} ->
          do_telemetry(:drop, source)
          {events, errors}

        %{pipeline_error: %_{message: message}, valid: false} ->
          do_telemetry(:invalid, source)
          {events, [message | errors]}

        le ->
          {[le | events], errors}
      end
    end)
  end

  defp maybe_mark_le_dropped_by_lql(%LogEvent{} = le, %Source{drop_lql_string: nil}), do: le
  defp maybe_mark_le_dropped_by_lql(%LogEvent{} = le, %Source{drop_lql_string: ""}), do: le
  defp maybe_mark_le_dropped_by_lql(%LogEvent{} = le, %Source{drop_lql_filters: []}), do: le

  defp maybe_mark_le_dropped_by_lql(%LogEvent{} = le, %Source{drop_lql_filters: filters}) do
    if SourceRouting.route_with_lql_rules?(le, %Rule{lql_filters: filters}) do
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

  defp maybe_broadcast_and_route(source, log_events) do
    if source.metrics.avg < 2 do
      Source.ChannelTopics.broadcast_new(log_events)
    end

    SourceRouting.route_to_sinks_and_ingest(log_events, source)

    :ok
  end

  # send to a specific backend
  defp dispatch_to_backends(source, %Backend{} = backend, log_events) do
    telemetry_metadata = %{backend_type: backend.type}

    :telemetry.span([:logflare, :backends, :ingest, :dispatch], telemetry_metadata, fn ->
      log_events = maybe_pre_ingest(source, backend, log_events)
      IngestEventQueue.add_to_table({source.id, backend.id}, log_events)

      :telemetry.execute(
        [:logflare, :backends, :ingest, :count],
        %{count: length(log_events)},
        %{backend_type: backend.type}
      )

      {:ok, telemetry_metadata}
    end)
  end

  defp dispatch_to_backends(source, nil, log_events) do
    backends = __MODULE__.Cache.list_backends(source_id: source.id)

    for backend <- [nil | backends] do
      {backend_id, backend_type} =
        if backend do
          {backend.id, backend.type}
        else
          {nil, SingleTenant.backend_type()}
        end

      telemetry_metadata = %{backend_type: backend_type}

      :telemetry.span([:logflare, :backends, :ingest, :dispatch], telemetry_metadata, fn ->
        log_events =
          if backend, do: maybe_pre_ingest(source, backend, log_events), else: log_events

        IngestEventQueue.add_to_table({source.id, backend_id}, log_events)

        :telemetry.execute(
          [:logflare, :backends, :ingest, :dispatch],
          %{count: length(log_events)},
          %{backend_type: backend_type}
        )

        {:ok, telemetry_metadata}
      end)
    end
  end

  defp maybe_pre_ingest(source, backend, events) do
    mod = Adaptor.get_adaptor(backend)

    if function_exported?(mod, :pre_ingest, 3) do
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
  @spec via_source(Source.t(), term()) :: tuple()
  @spec via_source(
          Source.t() | non_neg_integer(),
          module(),
          Backend.t() | non_neg_integer() | nil
        ) ::
          tuple()
  def via_source(%Source{id: sid}, mod, backend), do: via_source(sid, mod, backend)
  def via_source(source, mod, %Backend{id: bid}), do: via_source(source, mod, bid)
  def via_source(source_id, mod, backend_id), do: via_source(source_id, {mod, backend_id})

  def via_source(%Source{id: id}, process_id), do: via_source(id, process_id)

  def via_source(id, RecentEventsTouch) when is_number(id) do
    ts = DateTime.utc_now() |> DateTime.to_unix(:nanosecond)
    {:via, :syn, {:core, {RecentEventsTouch, id}, %{timestamp: ts}}}
  end

  def via_source(id, process_id) when is_number(id) do
    {:via, Registry, {SourceRegistry, {id, process_id}}}
  end

  @doc """
  Registers a unique backend-related process on the backend registry.
  """
  @spec via_backend(Backend.t() | non_neg_integer(), module()) :: tuple()
  def via_backend(%Backend{id: id}, mod), do: via_backend(id, mod)

  def via_backend(backend_id, mod) when is_number(backend_id) do
    {:via, Registry, {BackendRegistry, {mod, backend_id}}}
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
  Checks if the SourceSup for a given source has been started.
  """
  @spec source_sup_started?(Source.t() | non_neg_integer()) :: boolean()
  def source_sup_started?(%Source{id: id}), do: source_sup_started?(id)

  def source_sup_started?(id) when is_number(id) do
    Registry.lookup(SourceRegistry, {id, SourceSup}) != []
  end

  @doc """
  Starts a given SourceSup for a source. If already started, will return an error tuple.
  """
  @spec start_source_sup(Source.t()) :: :ok | {:error, :already_started}
  def start_source_sup(%Source{} = source) do
    case DynamicSupervisor.start_child(
           {:via, PartitionSupervisor, {SourcesSup, source.id}},
           SourceSup.child_spec(source)
         ) do
      {:ok, _pid} ->
        :ok

      {:error, {:already_started = reason, _pid}} ->
        {:error, reason}

      {:error, {:shutdown, {:failed_to_start_child, _mod, {:already_started = reason, _pid}}}} ->
        {:error, reason}
    end
  end

  @doc """
  Ensures that a the SourceSup is started.
  """
  @spec ensure_source_sup_started(Source.t()) :: :ok
  def ensure_source_sup_started(%Source{} = source) do
    if source_sup_started?(source) == false do
      case start_source_sup(source) do
        :ok -> :ok
        {:error, :already_started} -> :ok
      end
    else
      :ok
    end
  end

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
          :ok | {:error, :already_started} | {:error, :not_started}
  def restart_source_sup(%Source{} = source) do
    with :ok <- stop_source_sup(source),
         :ok <- start_source_sup(source) do
      :ok
    end
  end

  @doc """
  Uses the buffers cache in `PubSubRates.Cache` to determine if pending buffer is full.

  For sources with `default_ingest_backend_enabled? = true`:
    - Checks the system default backend queue
    - Checks user-designated default backends
    - Returns true if ANY of these are full
    - Falls back to checking all queues if no user default backends are configured

  For normal sources:
    - Checks ALL backend queues
    - Returns true only if ALL queues are full
  """
  @spec cached_local_pending_buffer_full?(Source.t()) :: boolean()
  def cached_local_pending_buffer_full?(%Source{
        id: source_id,
        default_ingest_backend_enabled?: true
      }) do
    default_backend_ids =
      __MODULE__.Cache.list_backends(source_id: source_id)
      |> Enum.filter(& &1.default_ingest?)
      |> MapSet.new(& &1.id)

    # Check system default backend (nil backend_id)
    system_default_full? = buffer_full_for_backend?(source_id, nil)

    # Check user-configured default backends
    user_defaults_full? = Enum.any?(default_backend_ids, &buffer_full_for_backend?(source_id, &1))

    system_default_full? || user_defaults_full?
  end

  def cached_local_pending_buffer_full?(%Source{id: source_id}) do
    PubSubRates.Cache.get_local_buffer(source_id, nil)
    |> Map.get(:queues, [])
    |> case do
      [] ->
        false

      queues ->
        Enum.all?(queues, fn {_key, v} -> v > @max_pending_buffer_len_per_queue end)
    end
  end

  @spec buffer_full_for_backend?(
          source_id :: non_neg_integer(),
          backend_id :: non_neg_integer() | nil
        ) ::
          boolean()
  defp buffer_full_for_backend?(source_id, backend_id) do
    case PubSubRates.Cache.get_local_buffer(source_id, backend_id) do
      %{queues: [_ | _] = queues} ->
        Enum.any?(queues, fn {_key, count} ->
          count > @max_pending_buffer_len_per_queue
        end)

      _ ->
        false
    end
  end

  @doc """
  Caches total buffer len. Includes ingested events that are awaiting cleanup.
  """
  @spec cache_local_buffer_lens(non_neg_integer(), non_neg_integer() | nil) ::
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
  """
  @spec list_recent_logs_local(Source.t() | pos_integer()) :: [LogEvent.t()]
  @spec list_recent_logs_local(Source.t() | pos_integer(), n :: number()) :: [LogEvent.t()]
  def list_recent_logs_local(source, n \\ 100)
  def list_recent_logs_local(%Source{id: id}, n), do: list_recent_logs_local(id, n)

  def list_recent_logs_local(source_id, n) when is_integer(source_id) do
    {:ok, events} = IngestEventQueue.fetch_events({source_id, nil}, n)

    events
    |> Enum.sort_by(& &1.body["timestamp"], &<=/2)
    |> Enum.take(-n)
  end

  @doc """
  Pipeline count resolution logic, for DynamicPipeline, shared across BigQuery and Clickhouse.
  """
  @spec handle_resolve_count(
          %{
            pipeline_count: non_neg_integer(),
            max_pipelines: non_neg_integer(),
            last_count_increase: NaiveDateTime.t() | nil,
            last_count_decrease: NaiveDateTime.t() | nil
          },
          %{
            {pos_integer(), pos_integer() | nil, reference() | nil} => non_neg_integer()
          },
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

  defp do_telemetry(:drop, source) do
    :telemetry.execute(
      [:logflare, :logs, :ingest_logs],
      %{drop: true},
      %{source_id: source.id, source_token: source.token}
    )
  end

  defp do_telemetry(:invalid, source) do
    :telemetry.execute(
      [:logflare, :logs, :ingest_logs],
      %{rejected: true},
      %{source_id: source.id, source_token: source.token}
    )
  end
end
