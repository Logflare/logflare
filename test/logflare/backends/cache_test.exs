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

  test "clear_list_backends_cache/1 removes the list_backends/1 entry", %{
    backend: backend,
    source: source
  } do
    backend_id = backend.id
    assert [%{id: ^backend_id}] = Backends.Cache.list_backends(source_id: source.id)

    cache_key = {:list_backends, [[source_id: source.id]]}
    assert {:cached, [%{id: ^backend_id}]} = Cachex.get!(Backends.Cache, cache_key)

    assert :ok = Backends.clear_list_backends_cache(source.id)
    assert is_nil(Cachex.get!(Backends.Cache, cache_key))
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
end
