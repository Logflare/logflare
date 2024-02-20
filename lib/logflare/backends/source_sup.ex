defmodule Logflare.Backends.SourceSup do
  @moduledoc false
  use Supervisor

  alias Logflare.Backends.Backend
  alias Logflare.Backends.CommonIngestPipeline
  alias Logflare.Backends
  alias Logflare.Source
  alias Logflare.Buffers.MemoryBuffer

  def start_link(%Source{} = source) do
    Supervisor.start_link(__MODULE__, source, name: Backends.via_source(source, __MODULE__))
  end

  def init(source) do
    specs =
      source
      |> Backends.list_backends()
      |> Enum.map(&Backend.child_spec(source, &1))

    children =
      [
        {MemoryBuffer, name: Backends.via_source(source, :buffer)},
        {CommonIngestPipeline, source}
      ] ++ specs

    Supervisor.init(children, strategy: :one_for_one)
  end
end
