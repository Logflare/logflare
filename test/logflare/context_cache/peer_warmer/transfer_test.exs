defmodule Logflare.ContextCache.PeerWarmer.TransferTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Logflare.ContextCache.PeerWarmer.CachexStore
  alias Logflare.ContextCache.PeerWarmer.Transfer

  @cache :peer_warmer_transfer_test_cache

  setup do
    start_supervised!(Supervisor.child_spec({Cachex, [@cache, []]}, id: @cache))
    :ok
  end

  test "transfers all entries in acknowledged chunks" do
    Cachex.put_many!(@cache, for(i <- 1..1_201, do: {i, i}))
    test_pid = self()

    import_fun = fn entries ->
      send(test_pid, {:chunk, length(entries)})
      length(entries)
    end

    assert {:ok, 1_201} = Transfer.run(node(), CachexStore, @cache, import_fun, 5_000)

    assert_received {:chunk, 500}
    assert_received {:chunk, 500}
    assert_received {:chunk, 201}
    refute_received {:chunk, _size}
  end

  test "counts the entries the importer kept" do
    Cachex.put_many!(@cache, for(i <- 1..10, do: {i, i}))

    assert {:ok, 0} = Transfer.run(node(), CachexStore, @cache, fn _entries -> 0 end, 5_000)
  end

  test "completes with no entries for an empty cache" do
    assert {:ok, 0} = Transfer.run(node(), CachexStore, @cache, &length/1, 5_000)
  end

  test "gives up at the deadline" do
    Cachex.put_many!(@cache, for(i <- 1..1_000, do: {i, i}))

    slow_import = fn entries ->
      Process.sleep(200)
      length(entries)
    end

    assert {:error, :timeout} = Transfer.run(node(), CachexStore, @cache, slow_import, 100)
  end

  test "fails when the peer can't be reached" do
    assert {:error, :rpc_failed} =
             Transfer.run(:"missing@127.0.0.1", CachexStore, @cache, &length/1, 5_000)
  end

  test "fails when the exporter goes down" do
    capture_log(fn ->
      assert {:error, :peer_down} =
               Transfer.run(node(), CachexStore, :missing_cache, &length/1, 5_000)
    end)
  end
end
