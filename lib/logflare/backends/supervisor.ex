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
    children = [
      Logflare.Backends.IngestEvents,
      Backends.Adaptor.PostgresAdaptor.Supervisor,
      {DynamicSupervisor, strategy: :one_for_one, name: Backends.SourcesSup},
      {Registry,
       name: Backends.SourceRegistry, keys: :unique, partitions: System.schedulers_online()},
      {Registry, name: Backends.SourceDispatcher, keys: :duplicate},
      {Registry,
       name: Logflare.Backends.BackendRegistry,
       keys: :unique,
       partitions: System.schedulers_online()}
    ]

    opts = [strategy: :one_for_one]

    Supervisor.init(children, opts)
  end
end
