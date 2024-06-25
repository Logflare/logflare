defmodule Logflare.Backends do
  @moduledoc false

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Backend
  alias Logflare.Backends.SourceDispatcher
  alias Logflare.Backends.SourceRegistry
  alias Logflare.Backends.SourcesSup
  alias Logflare.Backends.SourceSup
  alias Logflare.LogEvent
  alias Logflare.Repo
  alias Logflare.Source
  alias Logflare.Source
  alias Logflare.Sources
  alias Logflare.Logs
  alias Logflare.Logs.SourceRouting
  alias Logflare.SystemMetrics
  alias Logflare.PubSubRates
  alias Logflare.Cluster
  import Ecto.Query
  require Ex2ms

  defdelegate child_spec(arg), to: __MODULE__.Supervisor

  @max_buffer_len 5_000
  @doc """
  Lists `Backend`s for a given source.
  """
  @spec list_backends(Source.t()) :: list(Backend.t())
  def list_backends(%Source{id: id}) do
    from(b in Backend, join: s in assoc(b, :sources), where: s.id == ^id)
    |> Repo.all()
    |> Enum.map(fn b -> typecast_config_string_map_to_atom_map(b) end)
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
  Returns all backends set as a rule destination for a given source.

  ### Example
    iex>  list_backends_with_rules(source)
    [%Backend{...}, ...]
  """
  @spec list_backends_with_rules(Source.t()) :: [Backend.t()]
  def list_backends_with_rules(%Source{id: source_id}) do
    from(b in Backend, join: r in assoc(b, :rules), where: r.source_id == ^source_id)
    |> Repo.all()
    |> Enum.map(fn sb -> typecast_config_string_map_to_atom_map(sb) end)
  end

  def preload_rules(backends) do
    Repo.preload(backends, rules: [:source])
  end

  @doc """
  Creates a Backend for a given source.
  """
  @spec create_backend(map()) :: {:ok, Backend.t()} | {:error, Ecto.Changeset.t()}
  def create_backend(attrs) do
    backend =
      %Backend{}
      |> Backend.changeset(attrs)
      |> validate_config()
      |> Repo.insert()

    with {:ok, updated} <- backend do
      backend = Repo.preload(updated, :sources)
      Enum.each(backend.sources, &restart_source_sup(&1))
      {:ok, typecast_config_string_map_to_atom_map(updated)}
    end
  end

  @doc """
  Updates the config of a Backend.
  """
  @spec update_backend(Backend.t(), map()) :: {:ok, Backend.t()} | {:error, Ecto.Changeset.t()}
  def update_backend(%Backend{} = backend, attrs) do
    backend_config =
      backend
      |> Backend.changeset(attrs)
      |> validate_config()
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

  # common config validation function
  defp validate_config(%{valid?: true} = changeset) do
    type = Ecto.Changeset.get_field(changeset, :type)
    mod = Backend.adaptor_mapping()[type]

    Ecto.Changeset.validate_change(changeset, :config, fn :config, config ->
      case Adaptor.cast_and_validate_config(mod, config) do
        %{valid?: true} -> []
        %{valid?: false, errors: errors} -> for {key, err} <- errors, do: {:"config.#{key}", err}
      end
    end)
  end

  defp validate_config(changeset), do: changeset

  # common typecasting from string map to attom for config
  defp typecast_config_string_map_to_atom_map(nil), do: nil

  defp typecast_config_string_map_to_atom_map(%Backend{type: type} = backend) do
    mod = Backend.adaptor_mapping()[type]

    Map.update!(backend, :config, fn config ->
      (config || %{})
      |> mod.cast_config()
      |> Ecto.Changeset.apply_changes()
    end)
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
  Retrieves a backend by keyword filter.
  """
  @spec get_backend_by(keyword()) :: Backend.t() | nil
  def get_backend_by(kw) do
    if backend = Repo.get_by(Backend, kw) do
      typecast_config_string_map_to_atom_map(backend)
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
  @spec ingest_logs([log_param()], Source.t()) :: :ok
  @spec ingest_logs([log_param()], Source.t(), Backend.t() | nil) :: :ok
  def ingest_logs(event_params, source, backend \\ nil) do
    {log_events, errors} = split_valid_events(source, event_params)
    count = Enum.count(log_events)
    increment_counters(source, count)
    maybe_broadcast_and_route(source, log_events)
    push_recent_events(source, log_events)
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
    Logflare.Utils.Tasks.start_child(fn ->
      if source.metrics.avg < 5 do
        Source.ChannelTopics.broadcast_new(log_events)
      end

      SourceRouting.route_to_sinks_and_ingest(log_events)
    end)

    :ok
  end

  defp dispatch_to_backends(source, %Backend{} = backend, log_events) do
    adaptor_module = Adaptor.get_adaptor(backend)
    adaptor_module.ingest(nil, log_events, default_ingest_opts(source, backend))
    :ok
  end

  defp dispatch_to_backends(source, nil, log_events) do
    Registry.dispatch(SourceDispatcher, source.id, fn entries ->
      for {pid, {adaptor_module, :ingest, opts}} <- entries do
        # TODO: spawn tasks to do this concurrently
        adaptor_module.ingest(pid, log_events, opts)
      end
    end)

    :ok
  end

  @doc """
  Registers a backend for ingestion dispatching. Any opts that are provided are stored in the registry.

  If the `:register_for_ingest` field is `true`, the backend will be registered for ingest dispatching.

  The backend will not receive any dispatched events if it is set to false, and this function will be a `:noop`.


  Auto-populated options:
  - `:backend_id`
  - `:source_id`
  """
  @spec register_backend_for_ingest_dispatch(Source.t(), Backend.t(), keyword()) ::
          :ok
  def register_backend_for_ingest_dispatch(source, backend, opts \\ [])

  def register_backend_for_ingest_dispatch(
        source,
        %Backend{register_for_ingest: true} = backend,
        opts
      ) do
    mod = Adaptor.get_adaptor(backend)

    {:ok, _pid} =
      Registry.register(
        SourceDispatcher,
        source.id,
        {mod, :ingest, default_ingest_opts(source, backend) ++ opts}
      )

    :ok
  end

  def register_backend_for_ingest_dispatch(_source, _backend, _opts), do: :ok

  defp default_ingest_opts(source, backend) do
    [backend_id: backend.id, source_id: source.id]
  end

  @doc """
  Registers a unique source-related process on the source registry. Unique.
  For internal use only, should not be called outside of the `Logflare` namespace.

  ### Example
  iex> Backends.via_source(source,  __MODULE__, backend.id)
  iex> Backends.via_source(source, :buffer)
  """
  @spec via_source(Source.t(), term()) :: tuple()
  @spec via_source(Source.t(), term(), term()) :: tuple()
  def via_source(source, mod, id), do: via_source(source, {mod, id})

  def via_source(%Source{id: id}, process_id), do: via_source(id, process_id)

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
    {:via, _registry, {registry, via_id}} = via_source(source, module)

    case Registry.lookup(registry, via_id) do
      [{pid, _}] -> {:ok, pid}
      _ -> {:error, :not_started}
    end
  end

  @doc """
  checks if the SourceSup for a given source has been started.
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
    case DynamicSupervisor.start_child(SourcesSup, {SourceSup, source}) do
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
    case start_source_sup(source) do
      {:ok, _pid} -> :ok
      {:error, :already_started} -> :ok
      {:error, _} = err -> err
    end
  end

  @doc """
  Stops a given SourceSup for a source. if not started, it will return an error tuple.
  """
  @spec stop_source_sup(Source.t()) :: :ok | {:error, :not_started}
  def stop_source_sup(%Source{} = source) do
    with [{pid, _}] <- Registry.lookup(SourceRegistry, {source.id, SourceSup}),
         :ok <- DynamicSupervisor.terminate_child(SourcesSup, pid) do
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
  Checks if a local buffer is full.
  """
  def local_buffer_full?(%Source{} = source) do
    case PubSubRates.Cache.get_buffers(source.token, nil) do
      {:ok, %{} = buffers} ->
        buffer = get_in(buffers, [Node.self(), :len]) || 0
        buffer >= @max_buffer_len

      _ ->
        false
    end
  end

  @doc """
  Syncronously set the local buffer cache on the PubSub rates cache.
  Does not set the buffer len globally.
  Does not perform cluster broadcasting.
  Does not perform local broadcasting.

  if len arg is set to an integer, it will default setting the buffer len in the local node cache only
  """
  def set_local_buffer_len(source, backend, node \\ Node.self(), len)

  def set_local_buffer_len(%Source{} = source, %Backend{} = backend, node, len)
      when is_integer(len) do
    PubSubRates.Cache.cache_buffers(source.token, backend.token, %{node => %{len: len}})
  end

  def set_local_buffer_len(%Source{} = source, nil, node, len) when is_integer(len) do
    PubSubRates.Cache.cache_buffers(source.token, nil, %{node => %{len: len}})
  end

  @doc """
  Retrieves cluster-wide buffer size stored in cache for a given backend/source combination.
  """
  def buffer_len(%Source{} = source, backend \\ nil) do
    if backend do
      PubSubRates.Cache.get_cluster_buffers(source.token, backend.token)
    else
      PubSubRates.Cache.get_cluster_buffers(source.token)
    end
  end

  def push_recent_events(%Source{}, []), do: :ok

  def push_recent_events(%Source{id: source_id} = source, log_events) do
    existing = :ets.lookup(:recent_events, source_id)

    rem = 100 - Enum.count(log_events)

    to_keep =
      if rem > 0 do
        Enum.take(existing, -rem)
      else
        []
      end

    :ets.delete(:recent_events, source_id)

    objects =
      for event <- log_events do
        # don't cache source information
        {source_id, %{event | source: nil}}
      end

    :ets.insert(:recent_events, objects ++ to_keep)

    :ok
  end

  @doc """
  Lists the latest recent logs of all caches across the cluster.
  """
  @spec list_recent_events(Source.t()) :: [LogEvent.t()]
  def list_recent_events(%Source{} = source) do
    {results, _} = Cluster.Utils.rpc_multicall(__MODULE__, :list_recent_events_local, [source])

    List.flatten(results)
    |> Enum.sort_by(fn %{body: %{"timestamp" => ts}} -> ts end, &<=/2)
    |> Enum.take(-100)
  end

  @doc """
  Lists latest recent logs of only the local cache.
  """
  @spec list_recent_events_local(Source.t()) :: [LogEvent.t()]
  def list_recent_events_local(%Source{id: source_id}) do
    ms =
      Ex2ms.fun do
        {id, event} when id == ^source_id -> event
      end

    :ets.select(:recent_events, ms)
    |> Enum.sort_by(fn %{body: %{"timestamp" => ts}} -> ts end, &<=/2)
  end

  @doc """
  Retrieves the most recent event timestamp stored in recent_events cache locally
  """
  def get_latest_event_timestamp(%Source{} = source) do
    list_recent_events_local(source)
    |> Enum.reverse()
    |> List.first()
    |> case do
      %{body: %{"timestamp" => ts}} -> ts
      _ -> 0
    end
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
