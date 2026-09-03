defmodule Logflare.Sources.CacheWarmerTest do
  use Logflare.DataCase, async: false

  alias Logflare.Sources
  alias Logflare.Sources.Cache
  alias Logflare.Sources.CacheWarmer

  setup do
    insert(:plan)
    user = insert(:user)
    source = insert(:source, user: user, log_events_updated_at: NaiveDateTime.utc_now())
    Cachex.clear!(Cache)

    {:ok, source: source}
  end

  test "warms id and token entries under the keys the read path uses", %{source: source} do
    source_id = source.id

    assert %{id: ^source_id} = Cache.get_by(id: source.id)
    assert %{id: ^source_id} = Cache.get_by(token: source.token)
    assert {:ok, read_keys} = Cachex.keys(Cache)
    assert length(read_keys) == 2

    assert {:ok, pairs} = CacheWarmer.execute(nil)
    Cachex.clear!(Cache)
    assert {:ok, true} = Cachex.put_many(Cache, pairs)

    for read_key <- read_keys do
      assert {:ok, {:cached, %{id: ^source_id}}} = Cachex.get(Cache, read_key)
    end
  end

  test "serves warmed id and token entries without falling back to the database", %{
    source: source
  } do
    source_id = source.id

    assert {:ok, pairs} = CacheWarmer.execute(nil)
    assert {:ok, true} = Cachex.put_many(Cache, pairs)

    Sources
    |> reject(:get_by, 1)

    assert %{id: ^source_id} = Cache.get_by(id: source.id)
    assert %{id: ^source_id} = Cache.get_by(token: source.token)
  end
end
