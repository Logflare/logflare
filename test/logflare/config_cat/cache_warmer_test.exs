defmodule Logflare.ConfigCat.CacheWarmerTest do
  use ExUnit.Case, async: false
  use Mimic

  alias Logflare.ConfigCatCache
  alias Logflare.ConfigCat.CacheWarmer

  setup do
    start_supervised!(ConfigCatCache)
    :ok
  end

  describe "execute/1" do
    test "populates cache for all 100 buckets of each flag" do
      expect(ConfigCat, :get_value, 100, fn "key_values", false, user ->
        %ConfigCat.User{identifier: "key_values:" <> hash} = user
        hash_int = String.to_integer(hash)
        assert is_number(hash_int)
      end)

      assert {:ok, pairs} = CacheWarmer.execute(nil)
      assert length(pairs) == 100
      assert {"key_values:0", true} = Enum.find(pairs, fn {k, _} -> k == "key_values:0" end)
      assert {"key_values:99", false} = Enum.find(pairs, fn {k, _} -> k == "key_values:99" end)
    end
  end
end
