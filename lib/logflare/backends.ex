defmodule Logflare.Backends do
  @moduledoc false
  alias Logflare.Backends.{
    SourceBackend,
    SourceDispatcher,
    SourceRegistry,
    SourceSup,
    SourcesSup,
    RecentLogs,
    RecentLogsSup
  }

  alias Logflare.{Buffers.MemoryBuffer, Source, LogEvent, Repo}
  import Ecto.Query

  def list_source_backends(%Source{id: id}) do
    Repo.all(from sb in SourceBackend, where: sb.source_id == ^id)
  end

  def create_source_backend(%Source{} = source, type \\ :bigquery, config \\ nil) do
    source
    |> Ecto.build_assoc(:source_backends)
    |> SourceBackend.changeset(%{config: config, type: type})
    |> Repo.insert()
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
  def via_source(%Source{id: id}, process_id),
    do: {:via, Registry, {SourceRegistry, {id, process_id}}}

  @doc """
  Registers a unique source-related process on the source registry. Unique.
  For internal use only by adaptors, should not be called outside of the `Logflare` namespace.
  """
  @spec via_source_backend(SourceBackend.t(), term()) :: tuple()
  def via_source_backend(%SourceBackend{id: id, source_id: source_id}, process_id \\ nil) do
    identifier = {source_id, SourceBackend, id, process_id}
    {:via, Registry, {SourceRegistry, identifier}}
  end

  @spec source_sup_started?(Source.t()) :: boolean()
  def source_sup_started?(%Source{id: id}),
    do: Registry.lookup(SourceRegistry, {id, SourceSup}) != []

  @spec start_source_sup(Source.t()) :: :ok | {:error, :already_started}
  def start_source_sup(%Source{} = source) do
    with {:ok, _pid} <- DynamicSupervisor.start_child(SourcesSup, {SourceSup, source}) do
      :ok
    else
      {:error, {:already_started = reason, _pid}} -> {:error, reason}
    end
  end

  @spec stop_source_sup(Source.t()) :: :ok | {:error, :not_started}
  def stop_source_sup(%Source{} = source) do
    with [{pid, _}] <- Registry.lookup(SourceRegistry, {source.id, SourceSup}),
         :ok <- DynamicSupervisor.terminate_child(SourcesSup, pid) do
      :ok
    else
      _ -> {:error, :not_started}
    end
  end

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
    pid = ensure_recent_logs_started(source)
    RecentLogs.list(pid)
  end

  @doc """
  Pushes events into the global RecentLogs cache for a given source.any()
  Sets a global resource lock when pushing data into the cache.
  Performs a check to ensure that the cache is started. If not started yet globally, it will start the cache locally.
  """
  @spec push_recent_logs(Source.t(), [LogEvent.t()]) :: :ok
  def push_recent_logs(%Source{} = source, log_events) do
    pid = ensure_recent_logs_started(source)
    RecentLogs.set_lock(source)
    results = RecentLogs.push(pid, log_events)
    RecentLogs.del_lock(source)
    results
  end

  # checks if a recent logs cache is started. If not, starts the process.
  # returns the pid of the cache process if found
  defp ensure_recent_logs_started(%Source{} = source) do
    source
    |> RecentLogs.get_pid()
    |> case do
      nil -> start_recent_logs_cache(source)
      pid -> pid
    end
  end

  # starts the recent logs cache process locally for a given source
  defp start_recent_logs_cache(%Source{} = source) do
    case DynamicSupervisor.start_child(RecentLogsSup, {RecentLogs, source}) do
      {:ok, pid} -> pid
      {:error, {:already_started, pid}} -> pid
    end
  end
end
