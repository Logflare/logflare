defmodule Logflare.Backends.Spool.ProducerSup do
  @moduledoc false

  use Supervisor

  alias Logflare.Backends.Spool.EventQueue
  alias Logflare.Backends.Spool.ProducerPipeline

  @pipeline_name Logflare.Backends.Spool.ProducerPipeline

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl Supervisor
  def init(_opts) do
    # create the chunk queue's table before any producer/traffic exists for it
    EventQueue.init_table()

    children = [
      {ProducerPipeline, [name: @pipeline_name]}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
