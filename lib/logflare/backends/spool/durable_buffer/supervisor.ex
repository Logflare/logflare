defmodule Logflare.Backends.Spool.DurableBuffer.Supervisor do
  @moduledoc """
  Builds and starts the node's single `DurableBuffer` instance backing the
  spool producer path — `Backends.RotatingWal` wrapping `Backends.Cloud`
  for `:wal` buffer mode (default), or `Backends.Cloud` directly for
  `:mem` mode (config `:logflare, :spool, :buffer`).

  `max_batch_bytes`/`flush_delay_ms` here govern the *local* commit tier:
  how much accumulates, and how long a commit dwells once idle, before
  `DurableBuffer.Partition` hands a batch off. This backend is
  synchronous, so it never benefits from `DurableBuffer`'s adaptive dwell
  (that only applies to backends using the optional async commit
  contract) — `flush_delay_ms` is a fixed wait instead.
  `Backends.RotatingWal`'s own `max_batch_bytes` (rotation to cloud
  storage) and `max_rotation_interval_ms` are unrelated, coarser knobs
  configured directly on that backend.
  """

  alias Logflare.Backends.Spool.DurableBuffer.Backends.Cloud
  alias Logflare.Backends.Spool.DurableBuffer.Backends.RotatingWal
  alias Logflare.Backends.Spool.ProviderConfig

  @name __MODULE__.Buffer
  @default_partitions 4
  # How much accumulates before a local commit (fsync, for :wal buffer
  # mode).
  @default_max_batch_bytes 64 * 1024
  @default_flush_delay_ms 100

  @spec name() :: atom()
  def name, do: @name

  @doc "Every partition pid for this node's spool buffer, or [] if it isn't started."
  @spec partitions() :: [pid()]
  def partitions do
    case :persistent_term.get({DurableBuffer, name()}, nil) do
      nil ->
        []

      %{partitions: count} ->
        for index <- 0..(count - 1),
            pid = GenServer.whereis(DurableBuffer.partition_name(name(), index)),
            not is_nil(pid),
            do: pid
    end
  end

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(_opts) do
    spool_config = Application.get_env(:logflare, :spool, [])

    Supervisor.child_spec(
      {DurableBuffer,
       name: name(),
       backend: backend_spec(spool_config),
       partitions: Keyword.get(spool_config, :partitions, @default_partitions),
       max_batch_bytes: Keyword.get(spool_config, :max_batch_bytes, @default_max_batch_bytes),
       flush_delay_ms: Keyword.get(spool_config, :flush_delay_ms, @default_flush_delay_ms)},
      id: __MODULE__
    )
  end

  defp backend_spec(spool_config) do
    cloud = cloud_backend_spec(spool_config)

    case Keyword.get(spool_config, :buffer, :wal) do
      :mem ->
        cloud

      :wal ->
        opts =
          [wal_dir: wal_dir(spool_config), inner_backend: cloud]
          |> maybe_put(:worker_count, spool_config[:wal_worker_count])
          |> maybe_put(:max_batch_bytes, spool_config[:wal_max_batch_bytes])
          |> maybe_put(:max_rotation_interval_ms, spool_config[:wal_max_rotation_interval_ms])

        {RotatingWal, opts}
    end
  end

  # Falls back to a tmp dir so a plain boot with spool mode on doesn't
  # crash for lack of an explicit wal_dir — not durable across a real
  # restart, so anywhere the WAL is meant to survive one must set
  # SPOOL_WAL_DIR/:wal_dir explicitly (see cloudbuild/gce-startup.sh's
  # mount_wal_disk for how the dev/staging producer instances provide it).
  defp wal_dir(spool_config) do
    Keyword.get_lazy(spool_config, :wal_dir, fn ->
      Path.join(System.tmp_dir!(), "logflare_spool_wal")
    end)
  end

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)

  defp cloud_backend_spec(spool_config) do
    {storage_mod, queue_mod} = ProviderConfig.resolve_mods(spool_config)
    queue_ref = ProviderConfig.resolve_queue_ref(spool_config, queue_mod)

    {Cloud,
     bucket: Keyword.fetch!(spool_config, :bucket),
     storage_mod: storage_mod,
     queue_mod: queue_mod,
     queue_ref: queue_ref,
     compress: Keyword.get(spool_config, :compress, true)}
  end
end
