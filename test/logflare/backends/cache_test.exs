defmodule Logflare.Backends.CacheTest do
  @moduledoc false
  alias Logflare.Backends
  alias Logflare.Backends.CacheWarmer
  use Logflare.DataCase

  setup do
    insert(:plan)
    user = insert(:user)
    source = insert(:source, user: user)

    backend =
      insert(:backend,
        sources: [source]
      )

    {:ok, backend: backend, source: source, user: user}
  end

  test "warmer", %{user: user} do
    assert {:ok, []} = CacheWarmer.execute(nil)

    source =
      insert(:source,
        user: user,
        log_events_updated_at: NaiveDateTime.shift(NaiveDateTime.utc_now(), hour: -2)
      )

    backend = insert(:backend, sources: [source])

    assert {:ok, [_ | _] = pairs} = CacheWarmer.execute(nil)
    assert {:ok, true} = Cachex.put_many(Backends.Cache, pairs)

    Backends
    |> reject(:get_backend, 1)

    assert Backends.Cache.get_backend(backend.id)
  end

  test "clear_list_backends_cache/1 clears legacy and enabled source cache keys", %{
    source: source
  } do
    cache_keys = [
      {:list_backends, [[source_id: source.id]]},
      {:list_backends, [source_id: source.id]},
      {:list_backends, [[source_id: source.id, enabled: true]]},
      {:list_backends, [[rules_source_id: source.id]]},
      {:list_backends, [[rules_source_id: source.id, enabled: true]]}
    ]

    Enum.each(cache_keys, fn key ->
      assert {:ok, true} = Cachex.put(Backends.Cache, key, {:cached, []})
      assert {:cached, []} = Cachex.get!(Backends.Cache, key)
    end)

    assert :ok = Backends.clear_list_backends_cache(source.id)

    Enum.each(cache_keys, fn key ->
      assert is_nil(Cachex.get!(Backends.Cache, key))
    end)
  end
end
