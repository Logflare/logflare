defmodule Logflare.Backends.Spool.PartitionSupervisor do
  @moduledoc """
  Starts `partitions` (config) `Logflare.Backends.Spool.Partition`+`Committer`
  pairs, each registered under its own index in `PartitionRegistry` so
  callers can route to one (`random_partition/0`) and the dev dashboard can
  enumerate all of them (`partitions/0`) without depending on Broadway —
  replaces `ProducerPipeline`'s Broadway topology entirely.

  Each partition's own index doubles as its GCS/S3 key prefix (see
  `Committer.file_key/1`) — previously a separate random `:rand.uniform/1`
  roll independent of batcher concurrency; collapsing the two removes a
  redundant config axis.
  """

  use Supervisor

  alias Logflare.Backends.Spool.Partition
  alias Logflare.Backends.Spool.Queue
  alias Logflare.Backends.Spool.Storage

  require Logger

  @registry __MODULE__.Registry
  @default_batch_timeout 100
  @default_compression_algorithm :zstd

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @spec registry() :: atom()
  def registry, do: @registry

  @spec partitions() :: [pid()]
  def partitions do
    Registry.select(@registry, [{{:_, :"$1", :_}, [], [:"$1"]}])
  end

  # Picks from whichever partitions are actually registered right now, rather
  # than re-deriving an expected count from config — config can change (e.g.
  # in tests) without every already-running Partition's index membership
  # changing to match, and this way random_partition/0 can never pick an
  # index nothing is listening on.
  @spec random_partition() :: pid() | nil
  def random_partition do
    case partitions() do
      [] -> nil
      pids -> Enum.random(pids)
    end
  end

  @spec partition_pid(non_neg_integer()) :: pid() | nil
  def partition_pid(index) do
    case Registry.lookup(@registry, index) do
      [{pid, _}] -> pid
      [] -> nil
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
    format = Keyword.get(spool_config, :format, :ndjson)

    compression_algorithm =
      Keyword.get(spool_config, :compression_algorithm, @default_compression_algorithm)

    {storage_mod, queue_mod} = resolve_mods(spool_config)
    queue_ref = resolve_queue_ref(spool_config, queue_mod)

    partition_specs =
      for index <- 0..(partition_count() - 1) do
        opts = [
          name: {:via, Registry, {@registry, index}},
          index: index,
          bucket: bucket,
          batch_timeout: batch_timeout,
          compress: compress,
          format: format,
          compression_algorithm: compression_algorithm,
          storage_mod: storage_mod,
          queue_mod: queue_mod,
          queue_ref: queue_ref
        ]

        Supervisor.child_spec({Partition, opts}, id: {Partition, index})
      end

    children = [{Registry, keys: :unique, name: @registry} | partition_specs]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp resolve_queue_ref(spool_config, queue_mod) do
    name = Keyword.get(spool_config, :pubsub_topic) || Keyword.get(spool_config, :queue_name)

    case name do
      nil ->
        nil

      queue_name ->
        case queue_mod.resolve(queue_name) do
          {:ok, ref} ->
            ref

          {:error, reason} ->
            Logger.warning(
              "spool_partition_supervisor: could not resolve queue ref for #{queue_name}: #{inspect(reason)}"
            )

            nil
        end
    end
  end

  defp resolve_mods(spool_config) do
    provider = Keyword.get(spool_config, :provider, :aws)
    storage_mod = Keyword.get(spool_config, :storage_mod, default_storage_mod(provider))
    queue_mod = Keyword.get(spool_config, :queue_mod, default_queue_mod(provider))
    {storage_mod, queue_mod}
  end

  defp default_storage_mod(:gcp), do: Storage.GCS
  defp default_storage_mod(_), do: Storage.S3

  defp default_queue_mod(:gcp), do: Queue.PubSub
  defp default_queue_mod(_), do: Queue.SQS
end
