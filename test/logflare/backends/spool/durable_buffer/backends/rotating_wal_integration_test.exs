defmodule Logflare.Backends.Spool.DurableBuffer.Backends.RotatingWalIntegrationTest do
  @moduledoc """
  End-to-end proof that the two-tier commit split actually holds when a
  real `DurableBuffer` instance is wired to `RotatingWal`, wrapping a real
  `Cloud` backend: `DurableBuffer.append/3` (the fast tier — local fsync)
  must return long before the slow tier (the inner backend's
  upload+notify) settles, even though that upload was triggered by this
  exact append rotating the local file.
  """

  use ExUnit.Case, async: false

  import Mimic

  alias Logflare.Backends.Spool.DurableBuffer.Backends.Cloud
  alias Logflare.Backends.Spool.DurableBuffer.Backends.RotatingWal
  alias Logflare.Backends.Spool.Health
  alias Logflare.Backends.Spool.Queue.PubSub, as: QueueMod
  alias Logflare.Backends.Spool.Storage.GCS, as: StorageMod

  setup :set_mimic_global

  # RotatingWal.commit/4 and Cloud.commit/4 report to the real, global
  # :disk/:upload Health scopes on every call — reset both so a failure
  # here can't leak into another test file's own Health assertions.
  setup do
    on_exit(fn ->
      Health.report_recovery!(:disk)
      Health.report_recovery!(:upload)
    end)
  end

  defp wal_dir! do
    dir =
      Path.join(
        System.tmp_dir!(),
        "rotating_wal_integration_test_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(dir) end)
    dir
  end

  test "append/3 returns fast even though it triggers a rotation whose upload is slow" do
    test_pid = self()
    upload_delay_ms = 150

    stub(StorageMod, :put, fn _b, _k, body, _opts ->
      Process.sleep(upload_delay_ms)
      send(test_pid, {:put, body})
      {:ok, %{}}
    end)

    stub(QueueMod, :publish, fn ref, body ->
      send(test_pid, {:publish, ref, body})
      :ok
    end)

    name = :"rotating_wal_integration_#{System.unique_integer([:positive])}"

    backend =
      {
        RotatingWal,
        # Small enough that a single append already crosses it, forcing a
        # rotation (and thus the slow inner commit) on the very first call.
        wal_dir: wal_dir!(),
        max_batch_bytes: 4,
        worker_count: 1,
        inner_backend:
          {Cloud,
           bucket: "test-bucket",
           storage_mod: StorageMod,
           queue_mod: QueueMod,
           queue_ref: "projects/p/topics/t",
           compress: false}
      }

    start_supervised!({DurableBuffer, name: name, backend: backend, partitions: 1})

    {time_us, {:ok, _offset}} =
      :timer.tc(fn -> DurableBuffer.append(name, :some_key, "hello, rotating wal") end)

    # append/3 only waits on the LOCAL fsync — it must return in a small
    # fraction of the upload's artificial delay, not anywhere near it.
    assert time_us < upload_delay_ms * 1_000 / 2

    assert_receive {:put, body}, upload_delay_ms * 3
    assert {["hello, rotating wal"], _valid, ""} = DurableBuffer.WAL.decode_all(body)

    assert_receive {:publish, "projects/p/topics/t", notify_body}
    assert %{"event_count" => 1} = Jason.decode!(notify_body)
  end
end
