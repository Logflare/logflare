defmodule Logflare.SystemCacheTest do
  use ExUnit.Case, async: false

  import Mimic

  setup :verify_on_exit!

  setup do
    Cachex.clear(Logflare.SystemCache)
    :ok
  end

  describe "memory_utilization/0" do
    test "caches the result within TTL" do
      Logflare.System
      |> expect(:memory_utilization, 1, fn -> 0.42 end)

      assert Logflare.SystemCache.memory_utilization() == 0.42
      assert Logflare.SystemCache.memory_utilization() == 0.42
    end
  end
end
