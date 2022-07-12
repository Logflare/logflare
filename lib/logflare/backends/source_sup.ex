defmodule Logflare.Backends.SourceSup do
  use Supervisor

  alias Logflare.Backends.{SourceBackend}
  alias Logflare.Backends
  alias Logflare.Source

  def start_link(%Source{} = source) do
    Supervisor.start_link(__MODULE__, source, name: Backends.via_source(source))
  end

  def init(source) do
    source_backend_specs =
      source
      |> Backends.list_source_backends()
      |> Enum.map(&SourceBackend.child_spec/1)

    children =
      [
        # {Stack, [:hello]}
      ] ++ source_backend_specs

    Supervisor.init(children, strategy: :one_for_one)
  end

end
