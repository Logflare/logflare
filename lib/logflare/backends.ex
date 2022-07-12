defmodule Logflare.Backends do
  @moduledoc false
  alias Logflare.Backends.{
    SourceBackend,
    SourceDispatcher,
    SourceRegistry,
    SourceBackendRegistry
  }

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
  Dispatch a log events to a given source backend.

  It requires the source supervisor and registry to be running.
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

  @spec via_source(Source.t()) :: tuple()
  def via_source(%Source{id: id}), do: {:via, Registry, {SourceRegistry, id}}

  @spec via_source_backend(SourceBackend.t()) :: tuple()
  def via_source_backend(%SourceBackend{id: id}),
    do: {:via, Registry, {SourceBackendRegistry, id}}
end
