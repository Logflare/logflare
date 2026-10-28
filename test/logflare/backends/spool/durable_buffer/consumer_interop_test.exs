defmodule Logflare.Backends.Spool.DurableBuffer.ConsumerInteropTest do
  @moduledoc """
  Proves the existing, unmodified `ConsumerPipeline.QueueProducer` can
  consume what the new `DurableBuffer`-backed write path produces: real
  uploads/publishes captured from `Cloud` (mem mode) and `RotatingWal`
  (WAL mode) are replayed straight into a real `QueueProducer`, unchanged,
  and must decode back to the original segments.
  """

  use ExUnit.Case, async: false

  import Mimic

  alias Logflare.Backends.Spool.ConsumerPipeline.QueueProducer
  alias Logflare.Backends.Spool.DurableBuffer.Backends.Cloud
  alias Logflare.Backends.Spool.DurableBuffer.Backends.RotatingWal
  alias Logflare.Backends.Spool.Health
  alias Logflare.Backends.Spool.Queue.PubSub, as: QueueMod
  alias Logflare.Backends.Spool.Storage.GCS, as: StorageMod

  setup :set_mimic_global

  # Cloud.commit/4 and RotatingWal.commit/4 report to the real, global
  # :upload/:disk Health scopes on every call — reset both so a failure
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
        "durable_buffer_interop_test_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(dir) end)
    dir
  end

  # Matches Encoder.encode_raw_chunk/1's wire format — a 4-byte event-count
  # header followed by the etf-encoded records — since that's what the real
  # write path (Backends.ex) hands to DurableBuffer.append/3.
  defp raw_segment(ids) do
    records = Enum.map(ids, &%{"id" => &1})
    <<length(records)::32-big, :erlang.term_to_binary(records)::binary>>
  end

  defp capture_uploads(test_pid) do
    stub(StorageMod, :put, fn bucket, file_key, body, _opts ->
      send(test_pid, {:uploaded, bucket, file_key, body})
      {:ok, %{}}
    end)

    stub(QueueMod, :publish, fn queue_ref, msg ->
      send(test_pid, {:published, queue_ref, msg})
      :ok
    end)
  end

  # Wires the captured (file_key, body, queue message) straight into a real
  # QueueProducer and asserts it streams back exactly `expected_ids`.
  defp assert_consumed(bucket, file_key, body, queue_msg, expected_ids) do
    stub(StorageMod, :get, fn ^bucket, ^file_key -> {:ok, body} end)
    stub(QueueMod, :ack, fn _url, _handle -> :ok end)
    stub(QueueMod, :nack, fn _url, _handle -> :ok end)

    {:ok, agent} = Agent.start_link(fn -> [%{id: "h1", body: queue_msg}] end)

    stub(QueueMod, :receive, fn _url, _opts ->
      Agent.get_and_update(agent, fn
        [] -> {{:ok, []}, []}
        [msg | rest] -> {{:ok, [msg]}, rest}
      end)
    end)

    pid =
      start_supervised!(
        {QueueProducer,
         queue_url: "projects/p/subscriptions/s",
         bucket: bucket,
         storage_mod: StorageMod,
         queue_mod: QueueMod}
      )

    ids =
      [{pid, max_demand: 10}]
      |> GenStage.stream()
      |> Enum.take(1)
      |> Enum.flat_map(fn %{segment: segment} -> :erlang.binary_to_term(segment) end)
      |> Enum.map(& &1["id"])

    assert ids == expected_ids
  end

  test "mem mode (Cloud): QueueProducer decodes exactly what was appended" do
    test_pid = self()
    capture_uploads(test_pid)

    name = :"interop_mem_#{System.unique_integer([:positive])}"

    backend =
      {Cloud,
       bucket: "interop-bucket",
       storage_mod: StorageMod,
       queue_mod: QueueMod,
       queue_ref: "projects/p/topics/t",
       compress: false}

    start_supervised!({DurableBuffer, name: name, backend: backend, partitions: 1})

    {:ok, _offset} = DurableBuffer.append(name, :key, raw_segment(["e1", "e2", "e3"]))

    assert_receive {:uploaded, "interop-bucket", file_key, body}, 2000
    assert_receive {:published, "projects/p/topics/t", msg}, 2000

    assert_consumed("interop-bucket", file_key, body, msg, ["e1", "e2", "e3"])
  end

  test "WAL mode (RotatingWal): QueueProducer decodes exactly what was appended after rotation" do
    test_pid = self()
    capture_uploads(test_pid)

    name = :"interop_wal_#{System.unique_integer([:positive])}"

    backend =
      {
        RotatingWal,
        # Small enough that the append below already crosses it, forcing an
        # immediate rotation to the inner (cloud) backend.
        wal_dir: wal_dir!(),
        max_batch_bytes: 4,
        worker_count: 1,
        inner_backend:
          {Cloud,
           bucket: "interop-bucket",
           storage_mod: StorageMod,
           queue_mod: QueueMod,
           queue_ref: "projects/p/topics/t",
           compress: true}
      }

    start_supervised!({DurableBuffer, name: name, backend: backend, partitions: 1})

    {:ok, _offset} = DurableBuffer.append(name, :key, raw_segment(["e1"]))

    assert_receive {:uploaded, "interop-bucket", file_key, body}, 2000
    assert_receive {:published, "projects/p/topics/t", msg}, 2000

    assert_consumed("interop-bucket", file_key, body, msg, ["e1"])
  end
end
