defmodule Logflare.Backends.Spool.DurableBuffer.Backends.CloudTest do
  use ExUnit.Case, async: false

  import Mimic

  alias Logflare.Backends.Spool.DurableBuffer.Backends.Cloud, as: Backend
  alias Logflare.Backends.Spool.Health
  alias Logflare.Backends.Spool.Queue.PubSub, as: QueueMod
  alias Logflare.Backends.Spool.Storage.GCS, as: StorageMod

  setup :set_mimic_global

  # Backend.commit/4 reports to the real, global :upload Health scope on
  # every call — reset it so a failure here (or a prior test's) can't
  # leak into another test file's own Health assertions.
  setup do
    on_exit(fn -> Health.report_recovery!(:upload) end)
  end

  defp config(overrides \\ %{}) do
    Map.merge(
      %{
        bucket: "test-bucket",
        storage_mod: StorageMod,
        queue_mod: QueueMod,
        queue_ref: nil,
        compress: false,
        max_commit_attempts: 3,
        retry_delay_ms: 1
      },
      overrides
    )
  end

  # Each payload becomes one segment prefixed with a 4-byte event-count
  # header (see Encoder) — a count of 1 per payload, since each represents
  # a single logical event in these tests.
  defp batch(payloads) do
    Enum.map(payloads, fn payload ->
      {iodata, _size} = DurableBuffer.WAL.encode(<<1::32-big, payload::binary>>)
      iodata
    end)
  end

  defp decode_segments(body) do
    case DurableBuffer.WAL.decode_all(body) do
      {segments, _valid, ""} ->
        {:ok, Enum.map(segments, fn <<_count::32-big, etf::binary>> -> etf end)}

      {segments, _valid, rest} ->
        {:error, :corrupt, segments, rest}
    end
  end

  # DurableBuffer.append/3 frames whatever raw bytes it's given — the
  # event-count header a real caller (Encoder.encode_raw_chunk/1) would
  # add is our responsibility to add here too, so Cloud.commit/4's header
  # parsing has something valid to strip.
  defp headered(payload), do: <<1::32-big, payload::binary>>

  describe "commit/4" do
    test "uploads the framed batch and publishes a queue notification" do
      test_pid = self()

      stub(StorageMod, :put, fn "test-bucket", key, body, opts ->
        send(test_pid, {:put, key, body, opts})
        {:ok, %{}}
      end)

      stub(QueueMod, :publish, fn ref, body ->
        send(test_pid, {:publish, ref, body})
        :ok
      end)

      config = config(%{queue_ref: "projects/p/topics/t"})
      {:ok, state} = Backend.open(config, 0)

      assert {:ok, _new_state} = Backend.commit(state, batch(["one", "two"]), 0, {0, 2})

      assert_receive {:put, "0/" <> _ = key, body, [headers: %{"content-type" => ct}]}
      assert ct == "application/octet-stream"
      assert {:ok, ["one", "two"]} = decode_segments(body)

      assert_receive {:publish, "projects/p/topics/t", notify_body}
      assert %{"file_key" => ^key, "event_count" => 2} = Jason.decode!(notify_body)
    end

    test "compress: true sets the content-encoding header and produces a decompressible body" do
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, opts ->
        send(test_pid, {:put, body, opts})
        {:ok, %{}}
      end)

      config = config(%{compress: true})
      {:ok, state} = Backend.open(config, 0)

      assert {:ok, _state} = Backend.commit(state, batch(["hello"]), 0, {0, 1})

      assert_receive {:put, compressed_body, [headers: %{"content-encoding" => "zstd"}]}

      assert {:ok, ["hello"]} =
               compressed_body |> :ezstd.decompress() |> decode_segments()
    end

    test "no queue_ref means no publish call at all" do
      test_pid = self()
      stub(StorageMod, :put, fn _b, _k, _body, _opts -> {:ok, %{}} end)
      reject(&QueueMod.publish/2)

      config = config(%{queue_ref: nil})
      {:ok, state} = Backend.open(config, 0)

      assert {:ok, _state} = Backend.commit(state, batch(["one"]), 0, {0, 1})
      refute_receive {:publish, _, _}
      send(test_pid, :done)
      assert_receive :done
    end

    test "retries on failure up to max_commit_attempts, then gives up" do
      test_pid = self()
      {:ok, counter} = Agent.start_link(fn -> 0 end)

      stub(StorageMod, :put, fn _b, _k, _body, _opts ->
        attempt = Agent.get_and_update(counter, &{&1, &1 + 1})
        send(test_pid, {:attempt, attempt})
        {:error, :timeout}
      end)

      config = config(%{max_commit_attempts: 3, retry_delay_ms: 1})
      {:ok, state} = Backend.open(config, 0)

      assert {:error, :timeout, _state} = Backend.commit(state, batch(["one"]), 0, {0, 1})

      assert_receive {:attempt, 0}
      assert_receive {:attempt, 1}
      assert_receive {:attempt, 2}
      refute_receive {:attempt, 3}
    end

    test "retries then succeeds" do
      test_pid = self()
      {:ok, counter} = Agent.start_link(fn -> 0 end)

      stub(StorageMod, :put, fn _b, _k, _body, _opts ->
        attempt = Agent.get_and_update(counter, &{&1, &1 + 1})
        send(test_pid, {:attempt, attempt})
        if attempt < 2, do: {:error, :timeout}, else: {:ok, %{}}
      end)

      config = config(%{max_commit_attempts: 5, retry_delay_ms: 1})
      {:ok, state} = Backend.open(config, 0)

      assert {:ok, _state} = Backend.commit(state, batch(["one"]), 0, {0, 1})
      assert_receive {:attempt, 0}
      assert_receive {:attempt, 1}
      assert_receive {:attempt, 2}
    end
  end

  describe "commit_async/5 and handle_message/2" do
    setup do
      prev_spool_config = Application.get_env(:logflare, :spool)
      Application.put_env(:logflare, :spool, max_spool_health_failures: 1)

      on_exit(fn ->
        Health.report_recovery!(:upload)

        if prev_spool_config do
          Application.put_env(:logflare, :spool, prev_spool_config)
        else
          Application.delete_env(:logflare, :spool)
        end
      end)
    end

    test "async?/1 reports Cloud as an async backend" do
      assert DurableBuffer.Backend.async?(Backend)
    end

    test "returns :pending immediately without waiting for the upload" do
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        Process.sleep(50)
        send(test_pid, {:put, body})
        {:ok, %{}}
      end)

      config = config(%{max_commit_attempts: 1})
      {:ok, state} = Backend.open(config, 0)

      {time_us, {:pending, new_state}} =
        :timer.tc(fn -> Backend.commit_async(state, batch(["one"]), 0, {0, 1}, make_ref()) end)

      assert time_us < 50_000
      assert map_size(new_state.inflight) == 1
      refute_receive {:put, _}, 10
      assert_receive {:put, _body}, 200
    end

    test "resolves the tag with :ok and reports upload health recovery once the upload settles" do
      stub(StorageMod, :put, fn _b, _k, _body, _opts -> {:ok, %{}} end)

      config = config(%{max_commit_attempts: 1})
      {:ok, state} = Backend.open(config, 0)
      tag = make_ref()

      assert {:pending, state} = Backend.commit_async(state, batch(["one"]), 0, {0, 1}, tag)
      assert map_size(state.inflight) == 1

      assert_receive {:backend, {^tag, :ok}}, 200
      assert {[{^tag, :ok}], new_state} = Backend.handle_message({tag, :ok}, state)
      assert new_state.inflight == %{}
      assert Health.healthy?(:upload) == true
    end

    test "resolves the tag with {:error, reason} and reports upload health failure after retries are exhausted" do
      stub(StorageMod, :put, fn _b, _k, _body, _opts -> {:error, :timeout} end)

      config = config(%{max_commit_attempts: 1, retry_delay_ms: 1})
      {:ok, state} = Backend.open(config, 0)
      tag = make_ref()

      assert {:pending, state} = Backend.commit_async(state, batch(["one"]), 0, {0, 1}, tag)

      assert_receive {:backend, {^tag, {:error, :timeout}}}, 200

      assert {[{^tag, {:error, :timeout}}], new_state} =
               Backend.handle_message({tag, {:error, :timeout}}, state)

      assert new_state.inflight == %{}
      assert Health.healthy?(:upload) == false
    end

    test "a :DOWN from the task being killed outright still resolves the tag, instead of wedging every later commit on the partition behind it" do
      stub(StorageMod, :put, fn _b, _k, _body, _opts ->
        # Never actually reached — the task is killed before this runs.
        {:ok, %{}}
      end)

      config = config(%{max_commit_attempts: 1})
      {:ok, state} = Backend.open(config, 0)
      tag = make_ref()

      assert {:pending, state} = Backend.commit_async(state, batch(["one"]), 0, {0, 1}, tag)
      assert [ref] = Map.keys(state.inflight)

      assert {[{^tag, {:error, {:task_down, :killed}}}], new_state} =
               Backend.handle_message({:DOWN, ref, :process, self(), :killed}, state)

      assert new_state.inflight == %{}
      assert Health.healthy?(:upload) == false
    end

    test "a :DOWN for a tag already resolved by its own completion message is a no-op" do
      stub(StorageMod, :put, fn _b, _k, _body, _opts -> {:ok, %{}} end)

      config = config(%{max_commit_attempts: 1})
      {:ok, state} = Backend.open(config, 0)
      tag = make_ref()

      assert {:pending, state} = Backend.commit_async(state, batch(["one"]), 0, {0, 1}, tag)
      assert [ref] = Map.keys(state.inflight)

      assert_receive {:backend, {^tag, :ok}}, 200
      assert {[{^tag, :ok}], state} = Backend.handle_message({tag, :ok}, state)

      assert {[], ^state} = Backend.handle_message({:DOWN, ref, :process, self(), :normal}, state)
    end

    test "an unexpected crash mid-upload still resolves the tag, instead of leaking the in-flight credit forever" do
      stub(StorageMod, :put, fn _b, _k, _body, _opts -> raise "boom" end)

      config = config(%{max_commit_attempts: 1})
      {:ok, state} = Backend.open(config, 0)
      tag = make_ref()

      assert {:pending, _state} = Backend.commit_async(state, batch(["one"]), 0, {0, 1}, tag)

      assert_receive {:backend, {^tag, {:error, %RuntimeError{message: "boom"}}}}, 200
    end
  end

  describe "via a real DurableBuffer instance" do
    defp start_buffer!(opts) do
      name = :"durable_buffer_spike_#{System.unique_integer([:positive])}"

      start_supervised!(
        {DurableBuffer,
         Keyword.merge(
           [name: name, backend: {Backend, Map.to_list(config())}, partitions: 1],
           opts
         )}
      )

      name
    end

    test "append/3 blocks until the batch is uploaded and the queue notified" do
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        send(test_pid, {:put, body})
        {:ok, %{}}
      end)

      stub(QueueMod, :publish, fn ref, body ->
        send(test_pid, {:publish, ref, body})
        :ok
      end)

      name =
        start_buffer!(
          backend: {Backend, config(%{queue_ref: "projects/p/topics/t"}) |> Map.to_list()}
        )

      assert {:ok, _offset} = DurableBuffer.append(name, :some_key, headered("hello"))

      assert_receive {:put, body}
      assert {:ok, ["hello"]} = decode_segments(body)
      assert_receive {:publish, "projects/p/topics/t", _notify_body}
    end

    test "append_async/3 does not block, but sync/3 waits for the eventual commit" do
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        # Slow enough to prove append_async/3 itself doesn't wait for this.
        Process.sleep(50)
        send(test_pid, {:put, body})
        {:ok, %{}}
      end)

      name = start_buffer!([])

      assert :ok = DurableBuffer.append_async(name, :some_key, headered("one"))
      refute_receive {:put, _body}, 10

      assert :ok = DurableBuffer.sync(name, :some_key)
      assert_received {:put, body}
      assert {:ok, ["one"]} = decode_segments(body)
    end

    test "concurrent appends group-commit into a single upload" do
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        send(test_pid, {:put, body})
        Process.sleep(20)
        {:ok, %{}}
      end)

      # An explicit dwell so all three concurrent appends below are
      # guaranteed to land in the pending batch before the first flush
      # fires — otherwise this race is inherently non-deterministic (the
      # adaptive default schedules the very first append's flush immediately).
      name = start_buffer!(flush_delay_ms: 20)

      tasks =
        for payload <- ["a", "b", "c"] do
          Task.async(fn -> DurableBuffer.append(name, :some_key, headered(payload)) end)
        end

      results = Enum.map(tasks, &Task.await/1)
      assert Enum.all?(results, &match?({:ok, offset} when is_integer(offset), &1))

      assert_receive {:put, body}
      assert {:ok, segments} = decode_segments(body)
      assert Enum.sort(segments) == ["a", "b", "c"]

      # One upload for all three concurrent appends — the whole point of
      # group commit.
      refute_receive {:put, _}, 50
    end
  end

  describe "unsupported callbacks" do
    test "stream/2 raises — consumption happens via the queue/storage fan-out" do
      assert_raise RuntimeError, ~r/does not support stream\/2/, fn ->
        Backend.stream(config(), 0)
      end
    end

    test "truncate/2 is a no-op" do
      {:ok, state} = Backend.open(config(), 0)
      assert {:ok, ^state} = Backend.truncate(state, 0)
    end
  end
end
