defmodule Logflare.ContextCache.PeerWarmer.TransferTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Logflare.Cache.CachexOps
  alias Logflare.ContextCache.PeerWarmer.Transfer

  defmodule TestCache do
    use Logflare.ContextCache
  end

  defmodule NotStartedCache do
    use Logflare.ContextCache
  end

  setup do
    start_supervised!(CachexOps.child_spec(TestCache, limit: nil))
    :ok
  end

  test "transfers all entries in acknowledged chunks" do
    put_entries(1..1_201)
    test_pid = self()

    import_fun = fn entries ->
      send(test_pid, {:chunk, length(entries)})
      length(entries)
    end

    assert {:ok, 1_201} = Transfer.run(node(), TestCache, import_fun, 5_000)

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

    assert {:ok, 1} = Transfer.run(node(), TestCache, import_fun, 5_000)
    assert_received {:entries, [{1, 1, ttl}]}
    assert is_integer(ttl)
  end

  test "counts the entries the importer kept" do
    put_entries(1..10)

    assert {:ok, 0} = Transfer.run(node(), TestCache, fn _entries -> 0 end, 5_000)
  end

  test "completes with no entries for an empty cache" do
    assert {:ok, 0} = Transfer.run(node(), TestCache, &length/1, 5_000)
  end

  test "gives up at the deadline" do
    put_entries(1..1_000)

    slow_import = fn entries ->
      Process.sleep(200)
      length(entries)
    end

    assert {:error, :timeout} = Transfer.run(node(), TestCache, slow_import, 100)
  end

  test "fails when the peer can't be reached" do
    assert {:error, :rpc_failed} =
             Transfer.run(:"missing@127.0.0.1", TestCache, &length/1, 5_000)
  end

  test "fails when the exporter goes down" do
    capture_log(fn ->
      assert {:error, :peer_down} = Transfer.run(node(), NotStartedCache, &length/1, 5_000)
    end)
  end

  defp put_entries(keys), do: TestCache.put_entries(for key <- keys, do: {key, key, nil})
end
