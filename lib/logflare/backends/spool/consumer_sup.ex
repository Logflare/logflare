defmodule Logflare.Backends.Spool.ConsumerSup do
  @moduledoc false

  use Supervisor

  alias Logflare.Backends.Spool.ConsumerPipeline

  @spec start_link(term()) :: {:ok, pid()} | {:error, term()}
  def start_link(_) do
    Supervisor.start_link(__MODULE__, [])
  end

  @impl Supervisor
  def init(_) do
    children = [
      {ConsumerPipeline, [name: ConsumerPipeline]}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
