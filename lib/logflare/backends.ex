defmodule Logflare.Backends do
  @moduledoc false
  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Backends.RecentLogs
  alias Logflare.Backends.RecentLogsSup
  alias Logflare.Backends.SourceBackend
  alias Logflare.Backends.SourceDispatcher
  alias Logflare.Backends.SourceRegistry
  alias Logflare.Backends.SourcesSup
  alias Logflare.Backends.SourceSup

  alias Logflare.Buffers.MemoryBuffer
  alias Logflare.LogEvent
  alias Logflare.Repo
  alias Logflare.Source

  import Ecto.Query

  @adaptor_mapping %{webhook: WebhookAdaptor, postgres: PostgresAdaptor}

  @doc """
  Lists `SourceBackend`s for a given source.
  """
  @spec list_source_backends(Source.t()) :: list(SourceBackend.t())
  def list_source_backends(%Source{id: id}) do
    from(sb in SourceBackend, where: sb.source_id == ^id)
    |> Repo.all()
    |> Enum.map(fn sb -> typecast_config_string_map_to_atom_map(sb) end)
  end

  @doc """
  Lists `SourceBackend`s by user
  """
  @spec list_source_backends_by_user_id(integer()) :: [SourceBackend.t()]
  def list_source_backends_by_user_id(id) when is_integer(id) do
    from(sb in SourceBackend, join: s in Source, where: s.user_id == ^id)
    |> Repo.all()
    |> Enum.map(fn sb -> typecast_config_string_map_to_atom_map(sb) end)
  end

  @doc """
  Creates a SourceBackend for a given source.
  """
  @spec create_source_backend(Source.t(), String.t(), map()) ::
          {:ok, SourceBackend.t()} | {:error, Ecto.Changeset.t()}
  def create_source_backend(%Source{} = source, type, config \\ %{}) do
    source_backend =
      source
      |> Ecto.build_assoc(:source_backends)
      |> SourceBackend.changeset(%{config: config, type: type})
      |> validate_config()
      |> Repo.insert()

    case source_backend do
      {:ok, updated} ->
        restart_source_sup(source)
        {:ok, typecast_config_string_map_to_atom_map(updated)}

      other ->
        other
    end
  end

  @doc """
  Updates the config of a SourceBackend.
  """
  @spec update_source_backend_config(SourceBackend.t(), map()) ::
          {:ok, SourceBackend.t()} | {:error, Ecto.Changeset.t()}
  def update_source_backend_config(%SourceBackend{} = source_backend, %{} = config) do
    source_backend = Repo.preload(source_backend, :source)

    source_backend_config =
      source_backend
      |> SourceBackend.changeset(%{config: config})
      |> validate_config()
      |> Repo.update()

    case source_backend_config do
      {:ok, updated} ->
        restart_source_sup(source_backend.source)
        {:ok, typecast_config_string_map_to_atom_map(updated)}

      other ->
        other
    end
  end

  # common config validation function
  defp validate_config(changeset) do
    type = Ecto.Changeset.get_field(changeset, :type)

    Ecto.Changeset.validate_change(changeset, :config, fn :config, config ->
      case @adaptor_mapping[type].cast_and_validate_config(config) do
        %{valid?: true} -> []
        %{valid?: false, errors: errors} -> for {key, err} <- errors, do: {:"config.#{key}", err}
      end
    end)
  end

  # common typecasting from string map to attom for config
  defp typecast_config_string_map_to_atom_map(nil), do: nil

  defp typecast_config_string_map_to_atom_map(%SourceBackend{type: type} = source_backend) do
    Map.update!(source_backend, :config, fn config ->
      mod = @adaptor_mapping[type]

      typecasted =
        config
        |> mod.cast_config()
        |> Ecto.Changeset.apply_changes()

      mod_struct = struct(mod, %{config: typecasted})
      mod_struct.config
    end)
  end

  @doc """
  Retrieves a SourceBackend by id.
  """
  @spec get_source_backend(integer()) :: SourceBackend.t() | nil
  def get_source_backend(id) do
    source_backend = Repo.get(SourceBackend, id)

    typecast_config_string_map_to_atom_map(source_backend)
  end

  @doc """
  Deletes a Sourcebackend
  """
  @spec delete_source_backend(SourceBackend.t()) :: {:ok, SourceBackend.t()}
  def delete_source_backend(%SourceBackend{} = sb) do
    Repo.delete(sb)
  end

  @doc """
  Adds log events to the source event buffer.
  The ingestion pipeline then pulls from the buffer and dispatches log events to the correct backends.
  """
  @type log_param :: map()
  @spec ingest_logs(list(log_param()), Source.t()) :: :ok
  def ingest_logs(log_events, source) do
    via = via_source(source, :buffer)
    MemoryBuffer.add_many(via, log_events)
    :ok
  end

  @doc """
  Dispatch log events to a given source backend.
  It requires the source supervisor and registry to be running.
  For internal use only, should not be called outside of the `Logflare` namespace.
  """
  def dispatch_ingest(log_events, source) do
    Registry.dispatch(SourceDispatcher, source.id, fn entries ->
      for {pid, {adaptor_module, :ingest}} <- entries do
        # TODO: spawn tasks to do this concurrently
        apply(adaptor_module, :ingest, [pid, log_events])
      end
    end)

    :ok
  end

  @doc """
  Registers a unique source-related process on the source registry. Unique.
  For internal use only, should not be called outside of the `Logflare` namespace.
  """
  @spec via_source(Source.t(), term()) :: tuple()
  def via_source(%Source{id: id}, process_id) do
    {:via, Registry, {SourceRegistry, {id, process_id}}}
  end

  @doc """
  Registers a unique source-related process on the source registry. Unique.
  For internal use only by adaptors, should not be called outside of the `Logflare` namespace.
  """
  @spec via_source_backend(SourceBackend.t(), term()) :: tuple()
  def via_source_backend(%SourceBackend{id: id, source_id: source_id}, process_id \\ nil) do
    identifier = {source_id, SourceBackend, id, process_id}
    {:via, Registry, {SourceRegistry, identifier}}
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
    case DynamicSupervisor.start_child(SourcesSup, {SourceSup, source}) do
      {:ok, _pid} -> :ok
      {:error, {:already_started = reason, _pid}} -> {:error, reason}
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
  Lists the latest recent logs of a cache.
  Performs a check to ensure that the cache is started. If not started yet globally, it will start the cache locally.
  """
  @spec list_recent_logs(Source.t()) :: [LogEvent.t()]
  def list_recent_logs(%Source{} = source) do
    source
    |> ensure_recent_logs_started()
    |> RecentLogs.list()
  end

  @doc """
  Pushes events into the global RecentLogs cache for a given source.any()
  Performs a check to ensure that the cache is started. If not started yet globally, it will start the cache locally.
  """
  @spec push_recent_logs(Source.t(), [LogEvent.t()]) :: :ok
  def push_recent_logs(%Source{} = source, log_events) do
    pid = ensure_recent_logs_started(source)
    RecentLogs.push(pid, log_events)
  end

  # checks if a recent logs cache is started. If not, starts the process.
  # returns the pid of the cache process if found
  defp ensure_recent_logs_started(%Source{} = source) do
    pid = RecentLogs.get_pid(source)

    case pid do
      nil -> start_recent_logs_cache(source)
      pid -> pid
    end
  end

  # starts the recent logs cache process locally for a given source
  defp start_recent_logs_cache(%Source{} = source) do
    :global.set_lock({RecentLogs, source.id})

    pid =
      case DynamicSupervisor.start_child(RecentLogsSup, {RecentLogs, source}) do
        {:ok, pid} -> pid
        {:error, {:already_started, pid}} -> pid
      end

    :global.del_lock({RecentLogs, source.id})
    pid
  end
end
