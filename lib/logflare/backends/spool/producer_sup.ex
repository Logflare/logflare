defmodule Logflare.Backends.Spool.ProducerSup do
  @moduledoc false

  use Supervisor

  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.Spool.ProducerPipeline

  @pipeline_name Logflare.Backends.Spool.ProducerPipeline

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl Supervisor
  def init(_opts) do
    # create the startup queue and its generation, before any producer/traffic exists
    # for this queues_key — avoids racing concurrent first-time inserts against each
    # other to lazily create the generation (see IngestEventQueue.current_generation_tid/1)
    IngestEventQueue.upsert_tid({:spool_producer, nil, nil})
    IngestEventQueue.current_generation_tid({:spool_producer, nil})

    children = [
      {ProducerPipeline, [name: @pipeline_name]}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
