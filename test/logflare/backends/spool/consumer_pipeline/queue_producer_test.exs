defmodule Logflare.Backends.Spool.ConsumerPipeline.QueueProducerTest do
  use ExUnit.Case, async: false

  import Mimic

  alias Logflare.Backends.Spool.ConsumerPipeline.QueueProducer
  alias Logflare.Backends.Spool.MemoryMonitor
  alias Logflare.Backends.Spool.Queue.PubSub, as: QueueMod
  alias Logflare.Backends.Spool.Storage.GCS, as: StorageMod

  setup :set_mimic_global

  setup do
    original = Application.get_env(:logflare, :spool)

    on_exit(fn ->
      if original do
        Application.put_env(:logflare, :spool, original)
      else
        Application.delete_env(:logflare, :spool)
      end
    end)

    # MemoryMonitor's stats() are cached in :persistent_term — global, and
    # NOT reset between tests or test files. Without this, a test here could
    # inherit a stale "throttled" value left by any test anywhere in the
    # suite that ran MemoryMonitor last, hanging tests that have nothing to
    # do with throttling (e.g. prefetch/happy-path tests below). Starting a
    # fresh, explicitly non-throttled MemoryMonitor for every test in this
    # file guarantees a known baseline; throttled!/0 overrides it as needed.
    Application.put_env(:logflare, :spool, spool_memory_limit_percent: 1.0, spool_max_ets_percent: 1.0)
    start_supervised!(MemoryMonitor)
    Process.sleep(50)

    :ok
  end

  defp start_producer(opts \\ []) do
    defaults = [
      queue_url: "projects/p/subscriptions/s",
      bucket: "test-bucket",
      storage_mod: StorageMod,
      queue_mod: QueueMod
    ]

    start_supervised!({QueueProducer, Keyword.merge(defaults, opts)})
  end

  defp queue_message(handle, file_key) do
    %{id: handle, body: Jason.encode!(%{"file_key" => file_key})}
  end

  defp ndjson_body(records) do
    records
    |> Enum.map(&Jason.encode!/1)
    |> Enum.join("\n")
    |> Kernel.<>("\n")
  end

  # Cross-process mutable queue: both the producer and its background
  # prefetch Task call queue_mod.receive concurrently.
  defp stub_queue(messages) do
    {:ok, agent} = Agent.start_link(fn -> messages end)

    stub(QueueMod, :receive, fn _url, _opts ->
      Agent.get_and_update(agent, fn
        [] -> {{:ok, []}, []}
        [msg | rest] -> {{:ok, [msg]}, rest}
      end)
    end)

    agent
  end

  defp stub_ack_nack(test_pid) do
    stub(QueueMod, :ack, fn _url, handle ->
      send(test_pid, {:acked, handle})
      :ok
    end)

    stub(QueueMod, :nack, fn _url, handle ->
      send(test_pid, {:nacked, handle})
      :ok
    end)
  end

  # contents: %{file_key => body} | %{file_key => :raise} to simulate a
  # crashing download (e.g. an uncaught exception during a prefetch).
  defp stub_storage(contents) do
    stub(StorageMod, :get, fn _bucket, key ->
      case Map.fetch(contents, key) do
        {:ok, :raise} -> raise "boom: #{key}"
        {:ok, body} -> {:ok, body}
        :error -> {:error, %Tesla.Env{status: 404}}
      end
    end)
  end

  # MemoryMonitor is a shared, globally-named singleton — the file's own
  # `setup` block already starts one instance per test (non-throttled by
  # default). These helpers mutate config and wait for THAT SAME instance's
  # own ~1s refresh timer to pick up the change; they must not try to start
  # a second one (would fail with {:already_started, _}).
  defp throttled! do
    Application.put_env(:logflare, :spool, spool_memory_limit_percent: 0.0, spool_max_ets_percent: 0.0)
    Process.sleep(1_100)
    assert MemoryMonitor.throttled?() == true
  end

  defp not_throttled! do
    Application.put_env(:logflare, :spool, spool_memory_limit_percent: 1.0, spool_max_ets_percent: 1.0)
    Process.sleep(1_100)
    assert MemoryMonitor.throttled?() == false
  end

  defp count_messages_within(tag, total_ms) do
    deadline = System.monotonic_time(:millisecond) + total_ms
    do_count_messages(tag, deadline, 0)
  end

  defp do_count_messages(tag, deadline, acc) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      acc
    else
      receive do
        ^tag -> do_count_messages(tag, deadline, acc + 1)
      after
        remaining -> acc
      end
    end
  end

  describe "happy path" do
    test "streams events from a file and acks the queue message once exhausted" do
      stub_ack_nack(self())
      stub_queue([queue_message("h1", "0/a.ndjson")])
      stub_storage(%{"0/a.ndjson" => ndjson_body([%{"id" => "e1"}, %{"id" => "e2"}])})

      pid = start_producer()

      events =
        GenStage.stream([{pid, max_demand: 10}])
        |> Enum.take(2)

      assert Enum.map(events, & &1["id"]) == ["e1", "e2"]
      assert_receive {:acked, "h1"}, 2000
    end
  end

  describe "throttling (memory pressure)" do
    test "schedules a fallback poll instead of dropping the trigger when throttled" do
      throttled!()
      stub_queue([])

      pid = start_producer()
      Task.async(fn -> GenStage.stream([{pid, max_demand: 1}]) |> Enum.take(1) end)

      Process.sleep(50)

      poll_timer = :sys.get_state(pid).state.poll_timer

      refute is_nil(poll_timer)
      assert Process.read_timer(poll_timer)
    end

    test "recovers automatically once memory pressure drops, without any new demand arriving" do
      throttled!()

      stub_ack_nack(self())
      stub_queue([queue_message("h1", "0/a.ndjson")])
      stub_storage(%{"0/a.ndjson" => ndjson_body([%{"id" => "e1"}])})

      pid = start_producer()

      # Subscribes once and sends its one-time demand — no further demand will
      # ever be sent, so recovery must come entirely from the producer's own
      # internal retry, not from an externally-triggered handle_demand call.
      task = Task.async(fn -> GenStage.stream([{pid, max_demand: 10}]) |> Enum.take(1) end)

      refute Task.yield(task, 200)

      not_throttled!()

      assert {:ok, [event]} = Task.yield(task, 2000)
      assert event["id"] == "e1"
    end
  end

  describe "poll loop de-duplication" do
    test "queue polling frequency stays bounded regardless of the number of concurrently-demanding consumers" do
      test_pid = self()

      stub(QueueMod, :receive, fn _url, _opts ->
        send(test_pid, :queue_receive_called)
        {:ok, []}
      end)

      pid = start_producer()

      tasks =
        for _ <- 1..5 do
          Task.async(fn -> GenStage.stream([{pid, max_demand: 1}]) |> Enum.take(1) end)
        end

      # @poll_interval is 1000ms. If every concurrent handle_demand call spawned
      # its own poll chain (the pre-fix bug), 5 subscribers would each also
      # reschedule their own timer on every "queue empty" tick, and the call
      # count would grow roughly with subscriber_count * elapsed/interval.
      # With de-duplication, it should track ~elapsed/interval regardless of
      # how many consumers are asking for demand.
      call_count = count_messages_within(:queue_receive_called, 2500)

      assert call_count <= 4

      Enum.each(tasks, &Task.shutdown(&1, :brutal_kill))
    end
  end

  describe "prefetch task crash resilience" do
    test "a crashing prefetch does not permanently stall the producer" do
      stub_ack_nack(self())

      stub_queue([
        queue_message("h1", "0/a.ndjson"),
        queue_message("h2", "0/b.ndjson"),
        queue_message("h3", "0/c.ndjson")
      ])

      stub_storage(%{
        "0/a.ndjson" => ndjson_body([%{"id" => "e1"}]),
        "0/b.ndjson" => :raise,
        "0/c.ndjson" => ndjson_body([%{"id" => "e3"}])
      })

      pid = start_producer()

      # File A streams fine. Its background prefetch (file B) crashes during
      # download — pre-fix, this left state.prefetch stuck at :running forever
      # and the producer never advanced to file C. Recovery from the crash
      # happens on the next scheduled poll (~1s), single continuous subscription
      # throughout so we're not resetting demand bookkeeping mid-test.
      events =
        GenStage.stream([{pid, max_demand: 10}])
        |> Enum.take(2)

      assert Enum.map(events, & &1["id"]) == ["e1", "e3"]
      assert_receive {:acked, "h1"}, 2000
    end

    test "a crashing blocking fetch does not kill the producer process" do
      stub_ack_nack(self())

      stub_queue([
        queue_message("h1", "0/bad.ndjson"),
        queue_message("h2", "0/good.ndjson")
      ])

      stub_storage(%{
        "0/bad.ndjson" => :raise,
        "0/good.ndjson" => ndjson_body([%{"id" => "e1"}])
      })

      pid = start_producer()
      ref = Process.monitor(pid)

      # Cold start: no current file, no prefetch — this fetch happens on the
      # synchronous/blocking path, not inside a Task. Pre-fix, the raise here
      # would propagate out of handle_info(:poll) and crash the GenStage process.
      [event] =
        GenStage.stream([{pid, max_demand: 10}])
        |> Enum.take(1)

      assert event["id"] == "e1"
      refute_received {:DOWN, ^ref, :process, ^pid, _reason}
    end
  end
end
