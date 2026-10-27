defmodule Logflare.Endpoints.ResolverRaceTest do
  use Logflare.DataCase

  alias Logflare.Endpoints
  alias Logflare.Endpoints.Resolver
  alias Logflare.Endpoints.ResultsCache

  setup do
    endpoint = insert(:endpoint, cache_duration_seconds: 60, proactive_requerying_seconds: 60)

    on_exit(fn ->
      for pid <- Resolver.list_caches(endpoint, :all_versions), do: ResultsCache.invalidate(pid)
    end)

    %{endpoint: endpoint}
  end

  test "a taken registration is ignored without returning a potentially stale PID", context do
    args = {context.endpoint, %{}, []}
    pid = start_supervised!({ResultsCache, args})
    assert ResultsCache.init(args) == :ignore
    assert GenServer.whereis(ResultsCache.name(context.endpoint, %{})) == pid
  end

  test "a lost start retries and returns the replacement PID", context do
    stub(ResultsCache, :start_link, fn args ->
      Mimic.call_original(ResultsCache, :start_link, [args])
    end)

    expect(ResultsCache, :start_link, fn _args -> :ignore end)
    pid = Resolver.resolve(context.endpoint, %{}, [])
    assert is_pid(pid)
    assert GenServer.whereis(ResultsCache.name(context.endpoint, %{})) == pid
  end

  test "repeated ignored starts return a controlled response with bounded retries", context do
    expect(ResultsCache, :start_link, 3, fn _args -> :ignore end)

    assert {:error, %{"status" => "CACHE_REPLACED"}} =
             Endpoints.run_cached_query(context.endpoint)
  end
end
