defmodule Logflare.Backends.SupervisorTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends

  # A supervisor stops its children in reverse start order, so the registries must start
  # before the pipelines named in them to outlive them on shutdown.
  test "starts the registries before the pipelines named in them" do
    {:ok, {_flags, specs}} = Backends.Supervisor.init([])
    ids = Enum.map(specs, & &1.id)

    position = fn id ->
      Enum.find_index(ids, &(&1 == id)) || flunk("#{inspect(id)} not started")
    end

    for registry <- [
          Backends.SourceRegistry,
          Backends.BackendRegistry,
          Backends.BufferProducer.InFlightRegistry
        ],
        pipelines <- [Backends.ConsolidatedSup, Backends.SourcesSup] do
      assert position.(registry) < position.(pipelines),
             "#{inspect(registry)} must start before #{inspect(pipelines)}"
    end
  end
end
