defmodule Logflare.Backends.Spool.CommitterTest do
  use ExUnit.Case, async: false

  import Mimic

  alias Logflare.Backends.Spool.Committer
  alias Logflare.Backends.Spool.Encoder
  alias Logflare.Backends.Spool.Framing
  alias Logflare.Backends.Spool.Queue.PubSub, as: QueueMod
  alias Logflare.Backends.Spool.Storage.GCS, as: StorageMod
  alias Logflare.TestUtils

  setup :set_mimic_global

  defp config(overrides \\ %{}) do
    Map.merge(
      %{
        bucket: "test-bucket",
        storage_mod: StorageMod,
        queue_mod: QueueMod,
        queue_ref: nil,
        format: :ndjson,
        compress: false,
        compression_algorithm: :gzip,
        index: 0
      },
      overrides
    )
  end

  # A sealed segment on disk — what Partition.roll/1 hands to
  # Committer.commit_async/6 via a body_thunk that reads it.
  defp sealed_file!(payload \\ "line\n") do
    path =
      Path.join(
        System.tmp_dir!(),
        "spool_committer_test_#{System.unique_integer([:positive])}.sealed"
      )

    File.write!(path, Framing.encode_segment(payload))
    on_exit(fn -> File.rm(path) end)
    path
  end

  defp file_thunk(path), do: fn -> File.read(path) end

  describe "successful commit" do
    test "uploads, notifies, and reports :commit_success with the caller's context" do
      test_pid = self()

      stub(StorageMod, :put, fn "test-bucket", key, body, opts ->
        send(test_pid, {:put, key, body, opts})
        {:ok, %{}}
      end)

      stub(QueueMod, :publish, fn ref, body ->
        send(test_pid, {:publish, ref, body})
        :ok
      end)

      TestUtils.attach_forwarder([:logflare, :backends, :pipeline, :handle_batch])
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :queue, :publish])
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :storage, :put])
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :producer, :batch])

      path = sealed_file!("one\n")
      config = config(%{queue_ref: "projects/p/topics/t"})

      assert {:ok, _pid} =
               Committer.commit_async(test_pid, file_thunk(path), 1, :size, path, config)

      assert_receive {:put, "0/" <> _rest = key, body, opts}
      assert [headers: %{"content-type" => "application/x-ndjson"}] = opts
      assert {:ok, ["one\n"]} = Framing.decode_segments(body)
      assert Encoder.file_key_version(key) == Encoder.current_version()

      assert_receive {:publish, "projects/p/topics/t", notify_body}
      assert %{"file_key" => ^key, "event_count" => 1} = Jason.decode!(notify_body)

      assert_receive {:commit_success, ^path}
      # Committer never touches the file itself — that's the caller's job.
      assert File.exists?(path)

      assert_receive {:telemetry_event, [:logflare, :backends, :pipeline, :handle_batch],
                      %{batch_size: 1, batch_trigger: :size}, %{backend_type: :spool_producer}}

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :queue, :publish], %{},
                      %{result: :ok}}

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :storage, :put], _,
                      %{format: :ndjson, result: :ok}}

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :producer, :batch],
                      %{batch_size: 1}, %{result: :ok, stage: nil}}
    end

    test "gzip+compress sets the content-encoding header" do
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, _body, opts ->
        send(test_pid, {:opts, opts})
        {:ok, %{}}
      end)

      path = sealed_file!()
      config = config(%{compress: true, compression_algorithm: :gzip})

      assert {:ok, _pid} =
               Committer.commit_async(test_pid, file_thunk(path), 1, :size, path, config)

      assert_receive {:opts, [headers: %{"content-encoding" => "gzip"}]}
    end

    test "context doesn't need to be a file path at all — an in-memory body_thunk works identically" do
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        send(test_pid, {:put, body}) && {:ok, %{}}
      end)

      body = Framing.encode_segment("in memory\n")
      froms = [make_ref(), make_ref()]

      assert {:ok, _pid} =
               Committer.commit_async(test_pid, fn -> {:ok, body} end, 1, :size, froms, config())

      assert_receive {:put, ^body}
      assert_receive {:commit_success, ^froms}
    end
  end

  describe "retrying on failure" do
    test "retries with a fixed delay up to max_commit_attempts, then gives up" do
      Application.put_env(:logflare, :spool, retry_delay_ms: 1, max_commit_attempts: 3)
      test_pid = self()
      {:ok, counter} = Agent.start_link(fn -> 0 end)

      stub(StorageMod, :put, fn _b, _k, _body, _opts ->
        attempt = Agent.get_and_update(counter, &{&1, &1 + 1})
        send(test_pid, {:put_attempt, attempt})
        {:error, :timeout}
      end)

      path = sealed_file!()

      assert {:ok, _pid} =
               Committer.commit_async(test_pid, file_thunk(path), 1, :size, path, config())

      assert_receive {:put_attempt, 0}
      assert_receive {:put_attempt, 1}
      assert_receive {:put_attempt, 2}
      refute_receive {:put_attempt, 3}

      assert_receive {:commit_failed, ^path, :timeout}

      # Never deleted by Committer regardless of outcome — the caller's job,
      # and giving up here doesn't lose it: it's still on disk.
      assert File.exists?(path)
    end

    test "retries with a fixed delay until the upload eventually succeeds" do
      Application.put_env(:logflare, :spool, retry_delay_ms: 1)
      test_pid = self()
      {:ok, counter} = Agent.start_link(fn -> 0 end)

      stub(StorageMod, :put, fn _b, _k, _body, _opts ->
        attempt = Agent.get_and_update(counter, &{&1, &1 + 1})
        send(test_pid, {:put_attempt, attempt})
        if attempt < 2, do: {:error, :timeout}, else: {:ok, %{}}
      end)

      path = sealed_file!()

      assert {:ok, _pid} =
               Committer.commit_async(test_pid, file_thunk(path), 1, :size, path, config())

      assert_receive {:put_attempt, 0}
      assert_receive {:put_attempt, 1}
      assert_receive {:put_attempt, 2}
      assert_receive {:commit_success, ^path}
    end

    test "a notify failure retries the same way, even though the upload succeeded" do
      Application.put_env(:logflare, :spool, retry_delay_ms: 1)
      test_pid = self()
      stub(StorageMod, :put, fn _b, _k, _body, _opts -> {:ok, %{}} end)
      {:ok, counter} = Agent.start_link(fn -> 0 end)

      stub(QueueMod, :publish, fn _ref, _body ->
        attempt = Agent.get_and_update(counter, &{&1, &1 + 1})
        send(test_pid, {:publish_attempt, attempt})
        if attempt < 1, do: {:error, :unavailable}, else: :ok
      end)

      path = sealed_file!()
      config = config(%{queue_ref: "projects/p/topics/t"})

      assert {:ok, _pid} =
               Committer.commit_async(test_pid, file_thunk(path), 1, :size, path, config)

      assert_receive {:publish_attempt, 0}
      assert_receive {:publish_attempt, 1}
      assert_receive {:commit_success, ^path}
    end

    test "logs and emits error telemetry on every failed attempt, not just the last" do
      Application.put_env(:logflare, :spool, retry_delay_ms: 1)
      {:ok, counter} = Agent.start_link(fn -> 0 end)

      stub(StorageMod, :put, fn _b, _k, _body, _opts ->
        attempt = Agent.get_and_update(counter, &{&1, &1 + 1})
        if attempt < 2, do: {:error, :timeout}, else: {:ok, %{}}
      end)

      TestUtils.attach_forwarder([:logflare, :backends, :spool, :producer, :batch])

      path = sealed_file!()

      assert {:ok, _pid} =
               Committer.commit_async(self(), file_thunk(path), 1, :size, path, config())

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :producer, :batch],
                      %{batch_size: 1}, %{result: :error, stage: :upload}}

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :producer, :batch],
                      %{batch_size: 1}, %{result: :error, stage: :upload}}

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :producer, :batch],
                      %{batch_size: 1}, %{result: :ok, stage: nil}}
    end
  end

  describe "unreadable body" do
    test "a sealed 'file' that can't be read (e.g. it's a directory) is dropped, not retried" do
      # A directory read fails at the OS level (:eisdir) regardless of
      # permissions/user — reliable even when tests run as root.
      dir =
        Path.join(
          System.tmp_dir!(),
          "spool_committer_unreadable_#{System.unique_integer([:positive])}"
        )

      File.mkdir_p!(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      TestUtils.attach_forwarder([:logflare, :backends, :spool, :committer, :read_error])

      assert {:ok, _pid} =
               Committer.commit_async(self(), file_thunk(dir), 1, :size, dir, config())

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :committer, :read_error],
                      %{count: 1}, %{reason: :eisdir}}

      assert_receive {:commit_failed, ^dir, :eisdir}
    end

    test "a body_thunk that itself returns {:error, _} is dropped the same way, not retried" do
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :committer, :read_error])

      assert {:ok, _pid} =
               Committer.commit_async(self(), fn -> {:error, :boom} end, 1, :size, :ctx, config())

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :committer, :read_error],
                      %{count: 1}, %{reason: :boom}}

      assert_receive {:commit_failed, :ctx, :boom}
    end
  end

  describe "corrupt body" do
    test "a body that fails CRC validation is dropped without ever being uploaded, not retried" do
      test_pid = self()
      stub(StorageMod, :put, fn _b, _k, body, _opts -> send(test_pid, {:put, body}) end)

      good_frame = Framing.encode_segment("one\n")
      <<len::32-big, crc::32-big, _payload::binary>> = good_frame
      corrupted = <<len::32-big, crc::32-big, "TAMPERED"::binary>>

      path =
        Path.join(
          System.tmp_dir!(),
          "spool_committer_corrupt_#{System.unique_integer([:positive])}.sealed"
        )

      File.write!(path, corrupted)
      on_exit(fn -> File.rm(path) end)

      TestUtils.attach_forwarder([:logflare, :backends, :spool, :committer, :read_error])

      assert {:ok, _pid} =
               Committer.commit_async(test_pid, file_thunk(path), 1, :size, path, config())

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :committer, :read_error],
                      %{count: 1}, %{reason: :corrupt_frame}}

      assert_receive {:commit_failed, ^path, :corrupt_frame}
      refute_receive {:put, _body}, 100

      # Never deleted by Committer regardless of outcome — the caller's job.
      assert File.exists?(path)
    end
  end
end
