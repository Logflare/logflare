defmodule Logflare.Backends.AdaptorSupervisor do
  @moduledoc """
  This module acts as a adaptor-level supervisor, where under a SourceSup there may be many Adaptors running from many different backends.

  This essentially is a supervision tree for a source-backend combination.
  """
  use Supervisor

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.IngestEventQueue

  def start_link({source, backend} = opts) do
    Supervisor.start_link(__MODULE__, opts,
      name: Backends.via_source(source, __MODULE__, backend)
    )
  end

  @impl Supervisor
  def init({source, backend}) do
    adaptor_module = Adaptor.get_adaptor(backend)
    # create the startup queue and its generation, before any producer/traffic exists
    # for this queues_key — avoids racing concurrent first-time inserts against each
    # other to lazily create the generation (see IngestEventQueue.current_generation_tid/1)
    IngestEventQueue.upsert_tid({source.id, backend.id, nil})
    IngestEventQueue.current_generation_tid({source.id, backend.id})

    children =
      [
        {IngestEventQueue.QueueJanitor, source: source, backend: backend},
        {adaptor_module, {source, backend}}
      ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
