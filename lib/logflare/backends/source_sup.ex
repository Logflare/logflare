defmodule Logflare.Backends.SourceSup do
  @moduledoc false
  use Supervisor

  alias Logflare.Backends.{SourceBackend, CommonIngestPipeline}
  alias Logflare.Backends
  alias Logflare.Source
  alias Logflare.Buffers.MemoryBuffer

  def start_link(%Source{} = source) do
    Supervisor.start_link(__MODULE__, source, name: Backends.via_source(source, __MODULE__))
  end

  def init(source) do
    source_backend_specs =
      source
      |> Backends.list_source_backends()
      |> Enum.map(&SourceBackend.child_spec/1)

    children =
      [
        # {Stack, [:hello]}
        {MemoryBuffer, name: Backends.via_source(source, :buffer)},
        {CommonIngestPipeline, source}
      ] ++ source_backend_specs

    Supervisor.init(children, strategy: :one_for_one)
  end
end
