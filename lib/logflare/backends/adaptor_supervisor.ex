defmodule Logflare.Backends.AdaptorSupervisor do
  @moduledoc """
  This module acts as a adaptor-level supervisor, where under a SourceSup there may be many Adaptors running from many different backends.

  This essentailly is a supervision tree for a source-backend combination.
  """
  use Supervisor

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.IngestEventQueue

  def start_link({source, backend} = opts) do
    backend_id = if backend, do: backend.id

    Supervisor.start_link(__MODULE__, opts,
      name: Backends.via_source(source.id, __MODULE__, backend_id)
    )
  end

  def init({source, backend}) do
    adaptor_module = Adaptor.get_adaptor(backend)

    children =
      [
        {IngestEventQueue.DemandWorker, source: source, backend: backend},
        {IngestEventQueue.QueueJanitor, source: source, backend: backend},
        {adaptor_module, {source, backend}}
      ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
