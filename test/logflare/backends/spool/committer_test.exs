defmodule Logflare.Backends.Spool.CommitterTest do
  use ExUnit.Case, async: false

  import Mimic

  alias Logflare.Backends.Spool.Committer
  alias Logflare.Backends.Spool.Framing
  alias Logflare.Backends.Spool.Queue.PubSub, as: QueueMod
  alias Logflare.Backends.Spool.Storage.GCS, as: StorageMod
  alias Logflare.TestUtils

  setup :set_mimic_global

  defp start_committer(opts) do
    defaults = [
      partition: self(),
      index: 0,
      bucket: "test-bucket",
      compress: false,
      format: :ndjson,
      compression_algorithm: :gzip,
      storage_mod: StorageMod,
      queue_mod: QueueMod,
      queue_ref: nil
    ]

    opts = Keyword.merge(defaults, opts)
    start_supervised!({Committer, opts})
  end

  defp entry(payload \\ "line\n", from \\ nil, retries \\ 0) do
    {from, Framing.encode_segment(payload), byte_size(payload), 1, retries}
  end

  # A real `from` is the {pid, tag} pair GenServer.call's own machinery
  # would construct — building one directly lets us assert on
  # GenServer.reply/2's resulting {tag, result} message without needing a
  # live process actually blocked in a call.
  defp fake_from, do: {self(), make_ref()}

  describe "successful commit" do
    test "concatenates segments, uploads, notifies, replies to callers, and signals commit_done" do
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

      committer = start_committer(queue_ref: "projects/p/topics/t")
      {_pid, ref} = from = fake_from()

      Committer.commit(committer, [entry("one\n", from)], 5, 1, :size)

      assert_receive {:put, "0/" <> _rest = key, body, opts}
      assert [headers: %{"content-type" => "application/x-ndjson"}] = opts
      assert {:ok, ["one\n"]} = Framing.decode_segments(body)

      assert_receive {:publish, "projects/p/topics/t", notify_body}
      assert %{"file_key" => ^key, "event_count" => 1} = Jason.decode!(notify_body)

      assert_receive {^ref, :ok}
      assert_receive :commit_done

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

      committer = start_committer(compress: true, compression_algorithm: :gzip)
      Committer.commit(committer, [entry()], 5, 1, :size)

      assert_receive {:opts, [headers: %{"content-encoding" => "gzip"}]}
    end
  end

  describe "commit failure" do
    test "an upload failure replies {:error, reason} and tags stage: :upload" do
      Application.put_env(:logflare, :spool, max_retries: 0)
      stub(StorageMod, :put, fn _b, _k, _body, _opts -> {:error, :timeout} end)

      TestUtils.attach_forwarder([:logflare, :backends, :spool, :producer, :batch])

      committer = start_committer([])
      {_pid, ref} = from = fake_from()
      Committer.commit(committer, [entry("one\n", from)], 5, 1, :size)

      assert_receive {^ref, {:error, :timeout}}
      assert_receive :commit_done

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :producer, :batch],
                      %{count: 1}, %{result: :error, stage: :upload}}
    end

    test "a notify failure replies {:error, reason} and tags stage: :notify, even though the upload succeeded" do
      Application.put_env(:logflare, :spool, max_retries: 0)
      stub(StorageMod, :put, fn _b, _k, _body, _opts -> {:ok, %{}} end)
      stub(QueueMod, :publish, fn _ref, _body -> {:error, :unavailable} end)

      TestUtils.attach_forwarder([:logflare, :backends, :spool, :producer, :batch])

      committer = start_committer(queue_ref: "projects/p/topics/t")
      {_pid, ref} = from = fake_from()
      Committer.commit(committer, [entry("one\n", from)], 5, 1, :size)

      assert_receive {^ref, {:error, :unavailable}}

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :producer, :batch],
                      %{count: 1}, %{result: :error, stage: :notify}}
    end

    test "fire-and-forget entries (from: nil) are silently dropped on failure, not replied to" do
      Application.put_env(:logflare, :spool, max_retries: 0)
      stub(StorageMod, :put, fn _b, _k, _body, _opts -> {:error, :timeout} end)

      committer = start_committer([])
      Committer.commit(committer, [entry("one\n", nil)], 5, 1, :size)

      assert_receive :commit_done
      refute_receive {:error, _}
    end
  end
end
