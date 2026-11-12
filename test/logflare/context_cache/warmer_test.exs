defmodule Logflare.ContextCache.WarmerTest do
  use Logflare.DataCase, async: false

  import Cachex.Spec
  import ExUnit.CaptureLog

  alias Logflare.Backends
  alias Logflare.Cache.CachexOps
  alias Logflare.ContextCache.Tombstones
  alias Logflare.ContextCache.Warmer
  alias Logflare.Rules
  alias Logflare.SourceSchemas
  alias Logflare.Sources
  alias Logflare.Users

  @warmed_caches [Backends.Cache, Rules.Cache, SourceSchemas.Cache, Sources.Cache, Users.Cache]

  describe "context cache warmers" do
    setup do
      original_config = Application.get_env(:logflare, CachexOps)
      on_exit(fn -> Application.put_env(:logflare, CachexOps, original_config) end)
    end

    test "refresh periodically by default" do
      Application.delete_env(:logflare, CachexOps)

      for cache <- @warmed_caches do
        assert [warmer(interval: interval)] = warmers(cache)
        assert is_integer(interval) and interval > 0
      end
    end

    test "run only on startup when refreshing is disabled" do
      Application.put_env(:logflare, CachexOps, warmer_refresh_enabled: false)

      for cache <- @warmed_caches do
        assert [warmer(interval: nil)] = warmers(cache)
      end
    end
  end

  describe "warm/2" do
    setup do
      Cachex.clear!(Tombstones.Cache)

      telemetry_ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:logflare, :context_cache, :warm, :stop]
        ])

      on_exit(fn -> :telemetry.detach(telemetry_ref) end)
    end

    test "returns the loaded entries" do
      pairs = [{{:get_by, [[id: 1]]}, {:cached, %{id: 1}}}]

      assert {:ok, ^pairs} = Warmer.warm(Sources.Cache, fn -> pairs end)
    end

    test "drops entries whose records were recently invalidated" do
      fresh = {{:get_by, [[id: 1]]}, {:cached, %{id: 1}}}
      busted = {{:get_by, [[id: 2]]}, {:cached, %{id: 2}}}
      Tombstones.Cache.put_tombstone(Sources.Cache, 2)

      assert {:ok, [^fresh]} = Warmer.warm(Sources.Cache, fn -> [fresh, busted] end)

      assert_received {[:logflare, :context_cache, :warm, :stop], _ref, _measurements,
                       %{cache: Sources.Cache, count: 1, dropped: 1}}
    end

    test "drops lists containing a recently invalidated record and keeps empty lists" do
      empty = {{:list_by_source_id, [1]}, {:cached, []}}
      busted = {{:list_by_source_id, [2]}, {:cached, [%{id: 10}, %{id: 11}]}}
      Tombstones.Cache.put_tombstone(Rules.Cache, 11)

      assert {:ok, [^empty]} = Warmer.warm(Rules.Cache, fn -> [empty, busted] end)
    end

    test "logs errors and returns :ignore" do
      log =
        capture_log(fn ->
          assert :ignore = Warmer.warm(Sources.Cache, fn -> raise "boom" end)
        end)

      assert log =~ "Error warming Logflare.Sources.Cache: boom"
    end
  end

  defp warmers(cache) do
    %{start: {Cachex, :start_link, [[^cache, opts]]}} = cache.child_spec(nil)
    Keyword.fetch!(opts, :warmers)
  end
end
