defmodule Logflare.ContextCache.PeerWarmer.NebulexStoreTest do
  use ExUnit.Case, async: false

  alias Logflare.ContextCache.PeerWarmer.NebulexStore
  alias Logflare.KeyValues.Cache.L1

  setup do
    L1.delete_all!()
    on_exit(fn -> L1.delete_all!() end)
  end

  test "writes, streams and counts entries of a cache level" do
    entries = [{{:lookup, [1, "a", nil]}, %{"v" => 1}}, {{:count, 1}, 1}]

    assert :ok = NebulexStore.put_entries(L1, entries)

    streamed = L1 |> NebulexStore.stream() |> Enum.to_list()

    assert Enum.sort(streamed) == Enum.sort(entries)

    assert streamed |> Enum.map(&NebulexStore.key/1) |> Enum.sort() == [
             {:count, 1},
             {:lookup, [1, "a", nil]}
           ]

    assert streamed |> Enum.map(&NebulexStore.value/1) |> Enum.sort() == [1, %{"v" => 1}]
    assert NebulexStore.size(L1) == 2
    assert NebulexStore.exists?(L1, {:count, 1})
    refute NebulexStore.exists?(L1, {:count, 2})
  end

  test "writing no entries is a no-op" do
    assert :ok = NebulexStore.put_entries(L1, [])
    assert NebulexStore.size(L1) == 0
  end
end
