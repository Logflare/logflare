defmodule Logflare.ContextCache.TombstonesTest do
  use ExUnit.Case, async: false

  alias Logflare.ContextCache.Tombstones
  alias Logflare.Sources

  setup do
    Cachex.clear!(Tombstones.Cache)
    :ok
  end

  describe "record/1" do
    test "busted primary keys" do
      for {pkey_or_kw, tombstone} <- [{123, 123}, {[id: 234, source_id: 1], 234}] do
        Tombstones.record([{Sources, pkey_or_kw}])

        assert Tombstones.Cache.tombstoned?(Sources.Cache, tombstone)
      end
    end

    test "busted keyword without a primary key" do
      Tombstones.record([{Sources, [source_id: 345]}])

      refute Tombstones.Cache.tombstoned?(Sources.Cache, 345)
    end
  end
end
