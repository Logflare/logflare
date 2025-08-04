defmodule Logflare.Backends do
  @moduledoc false

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Backend
  alias Logflare.Backends.SourceRegistry
  alias Logflare.Backends.SourcesSup
  alias Logflare.Backends.SourceSup
  alias Logflare.Backends.RecentEventsTouch
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.LogEvent
  alias Logflare.Repo
  alias Logflare.Source
  alias Logflare.SingleTenant
  alias Logflare.User
  alias Logflare.Sources
  alias Logflare.Logs
  alias Logflare.Logs.SourceRouting
  alias Logflare.SystemMetrics
  alias Logflare.PubSubRates
  alias Logflare.Cluster
  alias Logflare.Sources.Counters
  import Ecto.Query

  defdelegate child_spec(arg), to: __MODULE__.Supervisor

  @max_pending_buffer_len_per_queue 15_000

  @doc """
  Retrieves the hardcoded max pending buffer length of an individual queue
  """
  @spec max_buffer_queue_len() :: non_neg_integer()
  def max_buffer_queue_len(), do: @max_pending_buffer_len_per_queue

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

  def preload_rules(backends) do
    Repo.preload(backends, rules: [:source])
  end

  @doc """
  Preload alerts key for a given backend
  """
  def preload_alerts(backends) do
    Repo.preload(backends, [:alert_queries])
  end

  @doc """
  Creates a Backend for a given source.
  """
  @spec create_backend(map()) :: {:ok, Backend.t()} | {:error, Ecto.Changeset.t()}
  def create_backend(attrs) do
    backend =
      %Backend{}
      |> Backend.changeset(attrs)
      |> Repo.insert()

    with {:ok, updated} <- backend do
      backend = Repo.preload(updated, :sources)
      Enum.each(backend.sources, &restart_source_sup(&1))
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
  @spec update_backend(Backend.t(), map()) :: {:ok, Backend.t()} | {:error, Ecto.Changeset.t()}
  def update_backend(%Backend{} = backend, attrs) do
    alerts_modified = if Map.get(attrs, :alert_queries), do: true, else: false

    backend_config =
      backend
      |> Backend.changeset(attrs)
      |> then(fn changeset ->
        if alerts_modified do
          Ecto.Changeset.put_assoc(changeset, :alert_queries, Map.get(attrs, :alert_queries))
        else
          changeset
        end
      end)
      |> Repo.update()

    with {:ok, updated} <- backend_config do
      backend = Repo.preload(backend, :sources)
      Enum.each(backend.sources, &restart_source_sup(&1))
      {:ok, typecast_config_string_map_to_atom_map(updated)}
    end
  end

  @doc """
  Updates the backends of a source wholly. Does not work on partial data, all backends must be provided.
  """
  @spec update_source_backends(Source.t(), [Backend.t()]) ::
          {:ok, Source.t()} | {:error, Ecto.Changeset.t()}
  def update_source_backends(%Source{} = source, backends) do
    changeset =
      source
      |> Repo.preload(:backends)
      |> Ecto.Changeset.change()
      |> Ecto.Changeset.put_assoc(:backends, backends)

    with {:ok, _} = result <- Repo.update(changeset) do
      changes = Ecto.Changeset.get_change(changeset, :backends)

      if source_sup_started?(source) do
        for %Ecto.Changeset{action: action, data: backend} <- changes do
          case action do
            :update ->
              SourceSup.start_backend_child(source, backend)

            :replace ->
              SourceSup.stop_backend_child(source, backend)
          end
        end
      end

      result
    end
  end

  # common typecasting from string map to attom for config
  def typecast_config_string_map_to_atom_map(nil), do: nil

  def typecast_config_string_map_to_atom_map(%Backend{type: type} = backend) do
    mod = Backend.adaptor_mapping()[type]

    updated =
      Map.update!(backend, :config_encrypted, fn config ->
        (config || %{})
        |> mod.cast_config()
        |> Ecto.Changeset.apply_changes()
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
    |> Ecto.Changeset.change()
    |> Ecto.Changeset.foreign_key_constraint(:sources,
      name: "sources_backends_backend_id_fkey",
      message: "There are still sources connected to this backend"
    )
    |> Repo.delete()
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
        %LogEvent{source: %Source{}} = le ->
          le

        %LogEvent{} = le ->
          %{le | source: source}

        param ->
          LogEvent.make(param, %{source: source})
      end
      |> Logs.maybe_mark_le_dropped_by_lql()
      |> LogEvent.apply_custom_event_message()
      |> case do
        %{drop: true} = le ->
          do_telemetry(:drop, le)
          {events, errors}

        %{valid: false} = le ->
          do_telemetry(:invalid, le)
          {events, errors}

        %{pipeline_error: err} = le when err != nil ->
          {events, [le.pipeline_error.message | errors]}

        le ->
          {[le | events], errors}
      end
    end)
  end

  defp increment_counters(source, count) do
    Sources.Counters.increment(source.token, count)
    SystemMetrics.AllLogsLogged.increment(:total_logs_logged, count)
    :ok
  end

  defp maybe_broadcast_and_route(source, log_events) do
    if source.metrics.avg < 5 do
      Source.ChannelTopics.broadcast_new(log_events)
    end

    SourceRouting.route_to_sinks_and_ingest(log_events)

    :ok
  end

  # send to a specific backend
  defp dispatch_to_backends(source, %Backend{} = backend, log_events) do
    log_events = maybe_pre_ingest(source, backend, log_events)
    IngestEventQueue.add_to_table({source.id, backend.id}, log_events)
  end

  defp dispatch_to_backends(source, nil, log_events) do
    for backend <- [nil | __MODULE__.Cache.list_backends(source_id: source.id)] do
      log_events =
        if(backend, do: maybe_pre_ingest(source, backend, log_events), else: log_events)

      backend_id = Map.get(backend || %{}, :id)
      IngestEventQueue.add_to_table({source.id, backend_id}, log_events)
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
  @spec via_source(Source.t() | non_neg_integer(), module(), Backend.t() | non_neg_integer()) ::
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
    # ensure that v1 pipeline source is already down
    case DynamicSupervisor.start_child(
           {:via, PartitionSupervisor, {SourcesSup, source.id}},
           {SourceSup, source}
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
  Ensures that a the SourceSup is started. Only returns error tuple if not alreadt started
  """
  @spec ensure_source_sup_started(Source.t()) :: :ok | {:error, term()}
  def ensure_source_sup_started(%Source{} = source) do
    if source_sup_started?(source) == false do
      case start_source_sup(source) do
        :ok -> :ok
        {:ok, _pid} -> :ok
        {:error, :already_started} -> :ok
        {:error, _} = err -> err
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
  Uses the buffers cache in PubSubRates.Cache to determine if pending buffer is full.
  Much more performant than not using the cache.
  """
  def cached_local_pending_buffer_full?(%Source{id: id}),
    do: cached_local_pending_buffer_full?(id)

  def cached_local_pending_buffer_full?(source_id) when is_integer(source_id) do
    PubSubRates.Cache.get_local_buffer(source_id, nil)
    |> Map.get(:queues, [])
    |> case do
      [] ->
        false

      queues ->
        queues
        |> Enum.all?(fn {_key, v} -> v > @max_pending_buffer_len_per_queue end)
    end
  end

  @doc """
  Uses the buffers cache in `PubSubRates.Cache` to determine if pending buffer is full for default ingest backends only.
  """
  def cached_local_pending_buffer_full_default_ingest?(%Source{id: id}),
    do: cached_local_pending_buffer_full_default_ingest?(id)

  def cached_local_pending_buffer_full_default_ingest?(source_id) when is_integer(source_id) do
    default_backends = __MODULE__.Cache.list_backends(source_id: source_id, default_ingest?: true)

    case default_backends do
      [] ->
        cached_local_pending_buffer_full?(source_id)

      backends ->
        # Check each default ingest backend's buffer individually
        Enum.any?(backends, fn backend ->
          buffer_data = PubSubRates.Cache.get_local_buffer(source_id, backend.id)
          len = Map.get(buffer_data, :len, 0)
          len > @max_pending_buffer_len_per_queue
        end)
    end
  end

  @doc """
  Caches total buffer len. Includes ingested events that are awaiting cleanup.
  """
  @spec cache_local_buffer_lens(non_neg_integer(), non_neg_integer() | nil) ::
          {:ok, %{len: non_neg_integer(), queues: map()}}
  def cache_local_buffer_lens(source_id, backend_id \\ nil) do
    queues = IngestEventQueue.list_counts({source_id, backend_id})

    len = for({_k, v} <- queues, do: v) |> Enum.sum()

    stats = %{len: len, queues: queues}
    payload = %{Node.self() => stats}
    PubSubRates.Cache.cache_buffers(source_id, backend_id, payload)
    {:ok, stats}
  end

  @doc """
  Get local pending buffer len of a source/backend combination
  """
  @spec cached_local_pending_buffer_len(Source.t(), Backend.t() | nil) :: non_neg_integer()
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
  @spec list_recent_logs_local(Source.t()) :: [LogEvent.t()]
  @spec list_recent_logs_local(Source.t(), n :: number()) :: [LogEvent.t()]
  def list_recent_logs_local(source, n \\ 100)
  def list_recent_logs_local(%Source{id: id}, n), do: list_recent_logs_local(id, n)

  def list_recent_logs_local(source_id, n) do
    {:ok, events} = IngestEventQueue.fetch_events({source_id, nil}, n)

    events
    |> Enum.sort_by(& &1.body["timestamp"], &<=/2)
    |> Enum.take(-n)
  end

  defp do_telemetry(:drop, le) do
    :telemetry.execute(
      [:logflare, :logs, :ingest_logs],
      %{drop: true},
      %{source_id: le.source.id, source_token: le.source.token}
    )
  end

  defp do_telemetry(:invalid, le) do
    :telemetry.execute(
      [:logflare, :logs, :ingest_logs],
      %{rejected: true},
      %{source_id: le.source.id, source_token: le.source.token}
    )
  end

  defp do_telemetry(:buffer_full, le) do
    :telemetry.execute(
      [:logflare, :logs, :ingest_logs],
      %{buffer_full: true},
      %{source_id: le.source.id, source_token: le.source.token}
    )
  end

  defp do_telemetry(:unknown_error, le) do
    :telemetry.execute(
      [:logflare, :logs, :ingest_logs],
      %{unknown_error: true},
      %{source_id: le.source.id, source_token: le.source.token}
    )
  end
end
