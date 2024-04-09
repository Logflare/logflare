defmodule Logflare.Backends do
  @moduledoc false

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Backends.RecentLogs
  alias Logflare.Backends.RecentLogsSup
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
  alias Logflare.Source.RecentLogsServer
  alias Logflare.Logs
  alias Logflare.Logs.SourceRouting
  alias Logflare.SystemMetrics
  import Ecto.Query

  @adaptor_mapping %{
    webhook: WebhookAdaptor,
    postgres: PostgresAdaptor,
    bigquery: BigQueryAdaptor
  }

  defdelegate child_spec(arg), to: __MODULE__.Supervisor

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
    source
    |> Repo.preload(:backends)
    |> Ecto.Changeset.change()
    |> Ecto.Changeset.put_assoc(:backends, backends)
    |> Repo.update()
  end

  # common config validation function
  defp validate_config(%{valid?: true} = changeset) do
    type = Ecto.Changeset.get_field(changeset, :type)
    mod = @adaptor_mapping[type]

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
    mod = @adaptor_mapping[type]

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

  TODO: Perform syncronous parsing and validation of log event params.

  Once this function returns `:ok`, the events get dispatched to respective backend adaptor portions of the pipeline to be further processed.
  """
  @type log_param :: map()
  @spec ingest_logs([log_param()], Source.t()) :: :ok
  def ingest_logs(event_params, source) do
    :ok = Source.Supervisor.ensure_started(source)
    {log_events, errors} = split_valid_events(source, event_params)
    count = Enum.count(log_events)
    increment_counters(source, count)
    maybe_broadcast_and_route(source, log_events)
    RecentLogsServer.push(source, log_events)
    dispatch_to_backends(source, log_events)
    if Enum.empty?(errors), do: {:ok, count}, else: {:error, errors}
  end

  defp split_valid_events(source, event_params) do
    event_params
    |> Enum.reduce({[], []}, fn param, {events, errors} ->
      le =
        param
        |> case do
          %LogEvent{} = le ->
            le

          param ->
            LogEvent.make(param, %{source: source})
        end
        |> Logs.maybe_mark_le_dropped_by_lql()
        |> LogEvent.apply_custom_event_message()

      cond do
        le.drop ->
          do_telemetry(:drop, le)
          {events, errors}

        not le.valid ->
          do_telemetry(:invalid, le)
          {events, errors}

        le.pipeline_error ->
          {events, [le.pipeline_error.message | errors]}

        true ->
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

  defp dispatch_to_backends(source, log_events) do
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

  Auto-populated options:
  - `:backend_id`
  - `:source_id`
  """
  @spec register_backend_for_ingest_dispatch(Source.t(), Backend.t(), keyword()) ::
          :ok
  def register_backend_for_ingest_dispatch(source, backend, opts \\ []) do
    mod = Adaptor.get_adaptor(backend)

    {:ok, _pid} =
      Registry.register(
        SourceDispatcher,
        source.id,
        {mod, :ingest, [backend_id: backend.id, source_id: source.id] ++ opts}
      )

    :ok
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
  @spec source_sup_started?(Source.t()) :: boolean()
  def source_sup_started?(%Source{id: id}) do
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
  Lists the latest recent logs of all caches across the cluster.
  Performs a check to ensure that the cache is started. If not started yet globally, it will start the cache locally.
  """
  @spec list_recent_logs(Source.t()) :: [LogEvent.t()]
  def list_recent_logs(%Source{} = source) do
    RecentLogsServer.list_for_cluster(source.token)
  end

  @doc """
  Lists latest recent logs of only the local cache.
  """
  @spec list_recent_logs_local(Source.t()) :: [LogEvent.t()]
  def list_recent_logs_local(%Source{} = source) do
    RecentLogsServer.list(source.token)
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
