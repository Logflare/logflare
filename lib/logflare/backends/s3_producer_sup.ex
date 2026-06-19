defmodule Logflare.Backends.S3ProducerSup do
  @moduledoc false

  use Supervisor

  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.S3ProducerPipeline

  @pipeline_name Logflare.Backends.S3ProducerPipeline

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl Supervisor
  def init(_opts) do
    IngestEventQueue.upsert_tid({:s3_producer, nil, nil})

    children = [
      {S3ProducerPipeline, [name: @pipeline_name]}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
