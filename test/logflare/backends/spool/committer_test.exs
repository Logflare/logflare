defmodule Logflare.Backends.Spool.CommitterTest do
  use ExUnit.Case, async: false

  import Mimic

  alias Logflare.Backends.Spool.Committer
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

  # A sealed segment on disk — what Partition.roll/1 actually hands to
  # Committer.commit/5, not an in-memory list.
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

  describe "successful commit" do
    test "uploads, notifies, and reports :ok to the caller — without touching the file" do
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
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :storage, :put])
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :queue, :publish])
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :producer, :batch])

      path = sealed_file!("one\n")

      assert :ok =
               Committer.commit(
                 test_pid,
                 path,
                 1,
                 :size,
                 config(%{queue_ref: "projects/p/topics/t"})
               )

      assert_receive {:put, "0/" <> _rest = key, body, opts}
      assert [headers: %{"content-type" => "application/x-ndjson"}] = opts
      assert {:ok, ["one\n"]} = Framing.decode_segments(body)

      assert_receive {:publish, "projects/p/topics/t", notify_body}
      assert %{"file_key" => ^key, "event_count" => 1} = Jason.decode!(notify_body)

      assert_receive {:commit_result, ^path, :ok}
      # Committer never deletes the file — that's Partition's job.
      assert File.exists?(path)

      assert_receive {:telemetry_event, [:logflare, :backends, :pipeline, :handle_batch],
                      %{batch_size: 1, batch_trigger: :size}, %{backend_type: :spool_producer}}

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :storage, :put], _,
                      %{format: :ndjson, result: :ok}}

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :queue, :publish],
                      %{count: 1}, %{result: :ok}}

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :producer, :batch],
                      %{count: 1}, %{result: :ok, stage: nil}}
    end

    test "gzip+compress sets the content-encoding header" do
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, _body, opts ->
        send(test_pid, {:opts, opts})
        {:ok, %{}}
      end)

      path = sealed_file!()
      config = config(%{compress: true, compression_algorithm: :gzip})

      assert :ok = Committer.commit(test_pid, path, 1, :size, config)

      assert_receive {:opts, [headers: %{"content-encoding" => "gzip"}]}
    end
  end

  describe "retrying on failure" do
    test "retries with a fixed delay until the upload eventually succeeds, never giving up" do
      Application.put_env(:logflare, :spool, retry_delay_ms: 1)
      test_pid = self()
      {:ok, counter} = Agent.start_link(fn -> 0 end)

      stub(StorageMod, :put, fn _b, _k, _body, _opts ->
        attempt = Agent.get_and_update(counter, &{&1, &1 + 1})
        send(test_pid, {:put_attempt, attempt})
        if attempt < 2, do: {:error, :timeout}, else: {:ok, %{}}
      end)

      path = sealed_file!()

      assert :ok = Committer.commit(test_pid, path, 1, :size, config())

      assert_receive {:put_attempt, 0}
      assert_receive {:put_attempt, 1}
      assert_receive {:put_attempt, 2}
      assert_receive {:commit_result, ^path, :ok}

      # Never deleted by Committer, regardless of how many attempts it took.
      assert File.exists?(path)
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

      assert :ok = Committer.commit(test_pid, path, 1, :size, config)

      assert_receive {:publish_attempt, 0}
      assert_receive {:publish_attempt, 1}
      assert_receive {:commit_result, ^path, :ok}
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
      assert :ok = Committer.commit(self(), path, 1, :size, config())

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :producer, :batch],
                      %{count: 1}, %{result: :error, stage: :upload}}

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :producer, :batch],
                      %{count: 1}, %{result: :error, stage: :upload}}

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :producer, :batch],
                      %{count: 1}, %{result: :ok, stage: nil}}
    end
  end
end
