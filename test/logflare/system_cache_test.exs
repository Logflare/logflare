defmodule Logflare.SystemCacheTest do
  use ExUnit.Case, async: false

  import Cachex.Spec
  import ExUnit.CaptureLog
  import Mimic

  setup :verify_on_exit!

  alias Logflare.SystemCache
  alias Logflare.SystemCache.Warmer

  setup do
    Cachex.clear(SystemCache)
    :ok
  end

  describe "memory_utilization/0" do
    test "caches the result within TTL" do
      Logflare.System
      |> expect(:memory_utilization, 1, fn -> 0.42 end)

      assert SystemCache.memory_utilization() == 0.42
      assert SystemCache.memory_utilization() == 0.42
    end
  end

  test "does not require the warmer to finish during cache startup" do
    previous_env = Application.get_env(:logflare, :env)
    Application.put_env(:logflare, :env, :prod)
    on_exit(fn -> Application.put_env(:logflare, :env, previous_env) end)

    assert %{start: {Cachex, :start_link, [SystemCache, options]}} = SystemCache.child_spec(nil)
    assert [warmer_spec] = Keyword.fetch!(options, :warmers)
    refute warmer(warmer_spec, :required)
  end

  test "cache starts while the optional warmer handles monitor exits" do
    previous_env = Application.get_env(:logflare, :env)
    Application.put_env(:logflare, :env, :prod)
    on_exit(fn -> Application.put_env(:logflare, :env, previous_env) end)
    set_mimic_global()

    assert %{start: {Cachex, :start_link, [SystemCache, options]}} = SystemCache.child_spec(nil)
    assert [warmer_spec] = Keyword.fetch!(options, :warmers)

    cache_name = Module.concat(__MODULE__, IntegrationCache)
    warmer_name = Module.concat(__MODULE__, IntegrationWarmer)
    options = Keyword.put(options, :warmers, [warmer(warmer_spec, name: warmer_name)])
    test_pid = self()

    stub(Logflare.System, :memory_utilization, fn ->
      send(test_pid, :warmer_called)
      exit(:monitor_unavailable)
    end)

    log =
      capture_log([level: :warning], fn ->
        cache_pid = start_supervised!({Cachex, [cache_name, options]})

        assert_receive :warmer_called
        :sys.get_state(warmer_name)
        assert Process.alive?(cache_pid)
      end)

    assert log =~ "SystemCache warmer failed"
    assert log =~ "monitor_unavailable"
  end

  test "warmer treats monitor exits as a cache miss" do
    stub(Logflare.System, :memory_utilization, fn -> exit(:monitor_unavailable) end)

    log =
      capture_log([level: :warning], fn ->
        assert :ignore = Warmer.execute(nil)
      end)

    assert log =~ "SystemCache warmer failed"
    assert log =~ "monitor_unavailable"
  end
end
