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
end
