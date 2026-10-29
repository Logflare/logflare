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
      for {pkey_or_kw, tombstone} <- [
            {123, 123},
            {[id: 234, other: :info], 234},
            {%{id: 345, other: :info}, 345},
            {"uuid-456", "uuid-456"}
          ] do
        Tombstones.record([{Sources, pkey_or_kw}])

        assert Tombstones.Cache.tombstoned?(Sources.Cache, tombstone)
      end
    end
  end
end
