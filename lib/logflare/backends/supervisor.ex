defmodule Logflare.Backends.Supervisor do
  @moduledoc """
  Processes related to v2 ingestion pipelines
  """

  use Supervisor

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.BigQueryAdaptor

  def start_link(_) do
    Supervisor.start_link(__MODULE__, [])
  end

  @impl Supervisor
  def init(_) do
    base = System.schedulers_online()

    spool_config = Application.get_env(:logflare, :spool, [])
    spool_provider = Keyword.get(spool_config, :provider, :aws)

    producer_children = if Backends.spool_producer_mode?(), do: [Backends.Spool.ProducerSup], else: []
    consumer_children = if Backends.spool_consumer_mode?(), do: [Backends.Spool.ConsumerSup], else: []
    spool_goth_children = if spool_provider == :gcp, do: List.wrap(spool_goth_child_spec()), else: []

    dbg({spool_provider, Backends.spool_producer_mode?(), Backends.spool_consumer_mode?()})

    children =
      [
        Backends.IngestEventQueue,
        Backends.IngestEventQueue.BufferCacheWorker,
        Backends.IngestEventQueue.MapperJanitor,
        Backends.Adaptor.PostgresAdaptor.Supervisor,
        Backends.Adaptor.ClickHouseAdaptor.MappingConfigStore,
        Backends.Adaptor.ClickHouseAdaptor.NativeIngester.SchemaCache,
        Backends.Adaptor.ClickHouseAdaptor.NativeIngester.PoolSup,
        Backends.Adaptor.ClickHouseAdaptor.QueryConnectionSup,
        Backends.ConsolidatedSup,
        {PartitionSupervisor, child_spec: DynamicSupervisor, name: Backends.SourcesSup},
        {Registry,
         name: Backends.SourceRegistry, keys: :unique, partitions: max(round(base / 8), 1)},
        {Registry,
         name: Backends.BackendRegistry, keys: :unique, partitions: max(round(base / 8), 1)}
      ] ++ spool_goth_children ++ producer_children ++ consumer_children

    opts = [strategy: :one_for_one]

    Supervisor.init(children, opts)
  end

  defp spool_goth_child_spec do
    case Application.get_env(:goth, :json) do
      nil ->
        nil

      json ->
        {Goth, opts} = BigQueryAdaptor.goth_child_spec(json)
        # prefetch: :async so a slow/failed token fetch doesn't block supervisor startup
        {Goth, opts |> Keyword.put(:name, Logflare.Spool.Goth) |> Keyword.put(:prefetch, :async)}
    end
  end
end
