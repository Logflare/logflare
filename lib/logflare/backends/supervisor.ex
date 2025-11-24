defmodule Logflare.Backends.Supervisor do
  @moduledoc """
  Processes related to v2 ingestion pipelines
  """

  use Supervisor

  alias Logflare.Backends

  def start_link(_) do
    Supervisor.start_link(__MODULE__, [])
  end

  @impl Supervisor
  def init(_) do
    base = System.schedulers_online()

    children = [
      Backends.IngestEventQueue,
      Backends.IngestEventQueue.BufferCacheWorker,
      Backends.IngestEventQueue.MapperJanitor,
      Backends.Adaptor.PostgresAdaptor.Supervisor,
      Backends.Adaptor.ClickhouseAdaptor.QueryConnectionSup,
      {PartitionSupervisor, child_spec: DynamicSupervisor, name: Backends.SourcesSup},
      {Registry,
       name: Backends.SourceRegistry, keys: :unique, partitions: max(round(base / 8), 1)},
      {Registry,
       name: Backends.BackendRegistry, keys: :unique, partitions: max(round(base / 8), 1)}
    ]

    opts = [strategy: :one_for_one]

    Supervisor.init(children, opts)
  end
end
