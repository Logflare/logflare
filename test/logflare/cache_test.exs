defmodule Logflare.CacheTest do
  use ExUnit.Case, async: true

  defmodule RecordingImpl do
    @behaviour Logflare.Cache.Ops

    @impl true
    def healthy?(cache), do: record({:healthy?, cache}, true)
    @impl true
    def stats(cache), do: record({:stats, cache}, %{})
    @impl true
    def reset(cache), do: record({:reset, cache}, :ok)

    defp record(call, result) do
      send(self(), call)
      result
    end
  end

  defmodule CustomImplCache do
    use Logflare.Cache, impl: RecordingImpl
  end

  test "cache with a custom impl module" do
    assert CustomImplCache.healthy?()
    assert %{} = CustomImplCache.stats()
    assert :ok = CustomImplCache.reset()

    assert_received {:healthy?, CustomImplCache}
    assert_received {:stats, CustomImplCache}
    assert_received {:reset, CustomImplCache}
  end
end
