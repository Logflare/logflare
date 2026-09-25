defmodule Logflare.Backends.Adaptor.SyslogAdaptor.PipelineTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.SyslogAdaptor.Pipeline
  alias Logflare.Backends.Backend
  alias Logflare.Sources.Source

  test "accepts Broadway topology overrides" do
    source = %Source{id: System.unique_integer([:positive])}
    backend = %Backend{id: System.unique_integer([:positive])}
    name = Backends.via_source(source, Pipeline, backend)

    assert {:ok, pipeline} =
             Pipeline.start_link(
               [source: source, backend: backend, pool: self(), name: name],
               producer: [module: {Broadway.DummyProducer, []}],
               processors: [default: [concurrency: 1]],
               batchers: [],
               context: %{}
             )

    assert Process.alive?(pipeline)
    assert :ok = Broadway.stop(pipeline)
  end
end
