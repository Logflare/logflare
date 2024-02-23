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

  alias Logflare.Buffers.Buffer
  alias Logflare.Buffers.MemoryBuffer
  alias Logflare.LogEvent
  alias Logflare.Repo
  alias Logflare.Source

  import Ecto.Query

  @adaptor_mapping %{
    webhook: WebhookAdaptor,
    postgres: PostgresAdaptor,
    bigquery: BigQueryAdaptor
  }

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
  """
  @type log_param :: map()
  @spec ingest_logs([log_param()], Source.t()) :: :ok
  def ingest_logs(log_events, source) do
    via = via_source(source, :buffer)
    Buffer.add_many(MemoryBuffer, via, log_events)
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
        adaptor_module.ingest(pid, log_events)
      end
    end)

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

  def via_source(%Source{id: id}, process_id) do
    {:via, Registry, {SourceRegistry, {id, process_id}}}
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
