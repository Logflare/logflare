defmodule Logflare.Backends.S3ConsumerSup do
  @moduledoc false

  use Supervisor

  alias Logflare.Backends.S3ConsumerPipeline

  @spec start_link(term()) :: {:ok, pid()} | {:error, term()}
  def start_link(_) do
    Supervisor.start_link(__MODULE__, [])
  end

  @impl Supervisor
  def init(_) do
    children = [
      {S3ConsumerPipeline, [name: S3ConsumerPipeline]}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
