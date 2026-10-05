defmodule Logflare.Backends.Spool.DurableBuffer.Backends.RotatingWal.WorkerTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Spool.DurableBuffer.Backends.RotatingWal.Worker
  alias Logflare.Test.FakeDurableBufferBackend, as: FakeInnerBackend

  defp sealed_file!(contents \\ "hello") do
    path =
      Path.join(
        System.tmp_dir!(),
        "rotating_wal_worker_test_#{System.unique_integer([:positive])}.sealed"
      )

    File.write!(path, contents)
    on_exit(fn -> File.rm(path) end)
    path
  end

  test "successful commit deletes the sealed file" do
    test_pid = self()
    config = FakeInnerBackend.init_config(test_pid: test_pid, commit_result: :ok)
    {:ok, worker} = Worker.start(FakeInnerBackend, config, 3)

    path = sealed_file!("the body")
    Worker.commit_segment(worker, path)

    assert_receive {:committed, 3, "the body", 8}
    # Give the cast time to finish deleting before asserting.
    :timer.sleep(10)
    refute File.exists?(path)
  end

  test "failed commit leaves the sealed file on disk for the next recovery scan" do
    test_pid = self()

    config = FakeInnerBackend.init_config(test_pid: test_pid, commit_result: {:error, :timeout})

    {:ok, worker} = Worker.start(FakeInnerBackend, config, 0)

    path = sealed_file!()
    Worker.commit_segment(worker, path)

    assert_receive {:committed, 0, "hello", 5}
    :timer.sleep(10)
    assert File.exists?(path)
  end

  test "an unreadable segment is logged and does not crash the worker" do
    test_pid = self()
    config = FakeInnerBackend.init_config(test_pid: test_pid, commit_result: :ok)
    {:ok, worker} = Worker.start(FakeInnerBackend, config, 0)

    missing_path =
      Path.join(System.tmp_dir!(), "does_not_exist_#{System.unique_integer()}.sealed")

    ExUnit.CaptureLog.capture_log(fn ->
      Worker.commit_segment(worker, missing_path)
      :timer.sleep(10)
    end)

    refute_receive {:committed, _, _, _}
    assert Process.alive?(worker)
  end

  test "start/3 does not link the worker to the caller" do
    test_pid = self()
    config = FakeInnerBackend.init_config(test_pid: test_pid, commit_result: :ok)
    {:ok, worker} = Worker.start(FakeInnerBackend, config, 0)

    {:links, links} = Process.info(self(), :links)
    refute worker in links

    Worker.stop(worker)
  end

  test "each worker gets the sub_partition_index it was started with" do
    test_pid = self()
    config = FakeInnerBackend.init_config(test_pid: test_pid, commit_result: :ok)
    {:ok, worker_a} = Worker.start(FakeInnerBackend, config, 10)
    {:ok, worker_b} = Worker.start(FakeInnerBackend, config, 11)

    Worker.commit_segment(worker_a, sealed_file!("a"))
    Worker.commit_segment(worker_b, sealed_file!("b"))

    assert_receive {:committed, 10, "a", 1}
    assert_receive {:committed, 11, "b", 1}
  end
end
