defmodule Logflare.ContextCache.PeerWarmer.TransferTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Logflare.Cache.CachexOps
  alias Logflare.ContextCache.PeerWarmer.Transfer

  @accept {__MODULE__, :accept, [:meta]}

  defmodule TestCache do
    use Logflare.ContextCache
  end

  defmodule NotStartedCache do
    use Logflare.ContextCache
  end

  defmodule StallingCache do
    use Logflare.ContextCache

    @impl Logflare.ContextCache
    def entries do
      Stream.concat(
        Enum.map(1..500, &{&1, &1, nil}),
        Stream.map([:late], fn key ->
          Process.sleep(5_000)
          {key, key, nil}
        end)
      )
    end
  end

  setup do
    start_supervised!(CachexOps.child_spec(TestCache, limit: nil))
    :ok
  end

  test "transfers all entries in acknowledged chunks, returning the guard's metadata" do
    put_entries(1..1_201)
    test_pid = self()

    import_fun = fn entries ->
      send(test_pid, {:chunk, length(entries)})
      length(entries)
    end

    assert {:ok, 1_201, :meta} = Transfer.run(node(), TestCache, @accept, import_fun, 5_000)

    assert_received {:chunk, 500}
    assert_received {:chunk, 500}
    assert_received {:chunk, 201}
    refute_received {:chunk, _size}
  end

  test "sends entries as {key, value, ttl}" do
    put_entries([1])
    test_pid = self()

    import_fun = fn entries ->
      send(test_pid, {:entries, entries})
      length(entries)
    end

    assert {:ok, 1, :meta} = Transfer.run(node(), TestCache, @accept, import_fun, 5_000)
    assert_received {:entries, [{1, 1, ttl}]}
    assert is_integer(ttl)
  end

  test "returns the guard's refusal without sending entries" do
    put_entries(1..10)
    refuse = {__MODULE__, :refuse, [:not_eligible]}

    assert {:error, :not_eligible} =
             Transfer.run(node(), TestCache, refuse, fn _entries -> flunk("sent") end, 5_000)
  end

  test "counts the entries the importer kept" do
    put_entries(1..10)

    assert {:ok, 0, :meta} = Transfer.run(node(), TestCache, @accept, fn _entries -> 0 end, 5_000)
  end

  test "completes with no entries for an empty cache" do
    assert {:ok, 0, :meta} = Transfer.run(node(), TestCache, @accept, &length/1, 5_000)
  end

  test "gives up at the deadline" do
    put_entries(1..1_000)

    slow_import = fn entries ->
      Process.sleep(200)
      length(entries)
    end

    assert {:error, :timeout} = Transfer.run(node(), TestCache, @accept, slow_import, 100)
  end

  test "fails when the peer can't be reached" do
    assert {:error, :rpc_failed} =
             Transfer.run(:"missing@127.0.0.1", TestCache, @accept, &length/1, 5_000)
  end

  test "fails when the peer doesn't have the guard function" do
    missing_guard = {__MODULE__, :missing_guard, []}

    assert {:error, :rpc_failed} =
             Transfer.run(node(), TestCache, missing_guard, &length/1, 5_000)
  end

  test "gives up when the exporter stalls, well before the deadline" do
    started_at = System.monotonic_time(:millisecond)

    assert {:error, :timeout} =
             Transfer.run(node(), StallingCache, @accept, &length/1, to_timeout(second: 30))

    assert System.monotonic_time(:millisecond) - started_at < to_timeout(second: 3)
  end

  test "the exporter stops when the requester doesn't ack a chunk" do
    put_entries(1..1_000)
    ref = make_ref()

    assert {:ok, exporter, :meta} = Transfer.start_export(TestCache, @accept, self(), ref)
    monitor = Process.monitor(exporter)

    assert_receive {^ref, :chunk, _entries}
    assert_receive {:DOWN, ^monitor, :process, ^exporter, :normal}, to_timeout(second: 3)
    refute_received {^ref, :chunk, _entries}
  end

  test "fails when the exporter goes down" do
    capture_log(fn ->
      assert {:error, :peer_down} =
               Transfer.run(node(), NotStartedCache, @accept, &length/1, 5_000)
    end)
  end

  @spec accept(term()) :: {:ok, term()}
  def accept(meta), do: {:ok, meta}

  @spec refuse(term()) :: {:error, term()}
  def refuse(reason), do: {:error, reason}

  defp put_entries(keys), do: TestCache.put_entries(for key <- keys, do: {key, key, nil})
end
