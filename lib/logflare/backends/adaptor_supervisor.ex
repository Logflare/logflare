defmodule Logflare.Backends.AdaptorSupervisor do
  @moduledoc """
  This module acts as a adaptor-level supervisor, where under a ConsolidatedSup or SourceSup there may be many Adaptors running from many different backends.

  This essentially is a supervision tree for a source-backend combination, or a consolidated backend. This ensures that QueueJanitors are started for both types of queues.
  if a source is not provided, it implies that it is a conslidated backend.
  if a source is provided but no backend is provided, it implies that it is a source that is using the system default backend
  """
  use Supervisor

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.IngestEventQueue

  def start_link({source, backend} = opts) do
    Supervisor.start_link(__MODULE__, opts, name: name(source, backend))
  end

  @spec name(Source.t() | nil, Backend.t() | nil) :: {:via, module(), term()}
  def name(nil, backend), do: Backends.via_backend(backend, __MODULE__)
  def name(source, backend), do: Backends.via_source(source, __MODULE__, backend)

  @impl Supervisor
  @spec init({Source.t() | nil, Backend.t() | nil}) :: Supervisor.on_init()
  def init({source, backend}) do
    adaptor_module = Adaptor.get_adaptor(backend)
    # initialize startup queues
    if source do
      IngestEventQueue.upsert_tid({source.id, backend.id, nil})
    else
      IngestEventQueue.upsert_tid({:consolidated, backend.id, nil})
    end

    children =
      [
        {IngestEventQueue.QueueJanitor, source: source, backend: backend},
        {adaptor_module, {source, backend}}
      ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
