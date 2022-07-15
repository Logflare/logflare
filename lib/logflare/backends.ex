defmodule Logflare.Backends do
  @moduledoc false
  alias Logflare.Backends.{
    SourceBackend,
    SourceDispatcher,
    SourceRegistry,
    SourceBackendRegistry,
    SourceSup,
    SourcesSup,
    RecentLogs
  }

  alias Logflare.Buffers.MemoryBuffer
  alias Logflare.Source
  alias Logflare.Repo
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
  Registeres a unique source-related process on the source registry. Unique.
  For internal use only, should not be called outside of the `Logflare` namespace.
  """
  @spec via_source(Source.t(), term()) :: tuple()
  def via_source(%Source{id: id}, process_id),
    do: {:via, Registry, {SourceRegistry, {id, process_id}}}

  @spec via_source_backend(SourceBackend.t()) :: tuple()
  def via_source_backend(%SourceBackend{id: id}),
    do: {:via, Registry, {SourceBackendRegistry, id}}

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

  def list_recent_logs(%Source{} = source) do
    via_source(source, RecentLogs)
    |> GenServer.call(:list)
  end
end
