defmodule Logflare.ErlSysMonTest do
  use ExUnit.Case, async: true

  alias Logflare.ErlSysMon

  test "returns no source keys when the registry has not started" do
    registry = :unstarted_source_registry

    refute Process.whereis(registry)
    assert ErlSysMon.source_registry_keys(self(), registry) == []
  end

  test "reports keys from a live registry" do
    registry = :test_erl_sys_mon_source_registry
    start_supervised!({Registry, keys: :unique, name: registry})

    assert {:ok, _} = Registry.register(registry, {:source, 123}, :admission)
    assert ErlSysMon.source_registry_keys(self(), registry) == [{:source, 123}]
  end
end
