defmodule Logflare.Backends.Spool.PartitionSupervisor do
  @moduledoc """
  Starts `partitions` (config) `Partition` processes, each registered under
  its own index so callers can route to one (`random_partition/0`) and the
  dev dashboard can enumerate all of them (`partitions/0`). Every partition
  shares the same buffer type — local-disk WAL or in-memory (config
  `:buffer`, default `:mem`) — a node-wide choice, not a per-caller one.
  """

  use Supervisor

  alias Logflare.Backends.Spool.Buffer
  alias Logflare.Backends.Spool.Partition
  alias Logflare.Backends.Spool.ProviderConfig

  @registry __MODULE__.Registry
  @default_batch_timeout 1_000

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @spec partitions() :: [pid()]
  def partitions do
    Registry.select(@registry, [{{:_, :"$1", :_}, [], [:"$1"]}])
  end

  @spec random_partition() :: pid() | nil
  def random_partition do
    case partitions() do
      [] -> nil
      pids -> Enum.random(pids)
    end
  end

  @spec partition_count() :: pos_integer()
  def partition_count do
    Application.get_env(:logflare, :spool, []) |> Keyword.get(:partitions, 4)
  end

  @impl Supervisor
  def init(_opts) do
    spool_config = Application.get_env(:logflare, :spool, [])
    bucket = Keyword.fetch!(spool_config, :bucket)
    batch_timeout = Keyword.get(spool_config, :batch_timeout, @default_batch_timeout)
    compress = Keyword.get(spool_config, :compress, true)
    wal_dir = Keyword.get(spool_config, :wal_dir)
    buffer_mod = buffer_mod(spool_config)

    {storage_mod, queue_mod} = ProviderConfig.resolve_mods(spool_config)
    queue_ref = ProviderConfig.resolve_queue_ref(spool_config, queue_mod)

    partition_specs =
      for index <- 0..(partition_count() - 1)//1 do
        opts = [
          name: {:via, Registry, {@registry, index}},
          buffer_mod: buffer_mod,
          index: index,
          bucket: bucket,
          batch_timeout: batch_timeout,
          compress: compress,
          storage_mod: storage_mod,
          queue_mod: queue_mod,
          queue_ref: queue_ref,
          wal_dir: wal_dir
        ]

        Supervisor.child_spec({Partition, opts}, id: {Partition, index})
      end

    children = [{Registry, keys: :unique, name: @registry} | partition_specs]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp buffer_mod(spool_config) do
    case Keyword.get(spool_config, :buffer, :mem) do
      :wal -> Buffer.WAL
      :mem -> Buffer.Mem
    end
  end
end
