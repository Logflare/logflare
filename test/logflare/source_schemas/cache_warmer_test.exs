defmodule Logflare.SourceSchemas.CacheWarmerTest do
  use Logflare.DataCase, async: false

  alias Logflare.SourceSchemas.Cache
  alias Logflare.SourceSchemas.CacheWarmer

  setup do
    Cachex.clear!(Cache)
    :ok
  end

  test "warms source schemas under their read-through cache keys" do
    user = insert(:user)
    source = insert(:source, user: user, log_events_updated_at: NaiveDateTime.utc_now())
    source_schema = insert(:source_schema, source: source)
    cache_key = {:get_source_schema_by, [source_id: source.id]}

    assert {:ok, pairs} = CacheWarmer.execute(nil)
    assert {^cache_key, {:cached, %{id: source_schema_id}}} = List.keyfind(pairs, cache_key, 0)
    assert source_schema_id == source_schema.id
    assert {:ok, true} = Cachex.put_many(Cache, pairs)

    # Check Cachex directly so read-through fallback cannot hide malformed warmer output.
    assert {:ok, {:cached, %{id: ^source_schema_id}}} = Cachex.get(Cache, cache_key)

    assert %{id: ^source_schema_id} = Cache.get_source_schema_by(source_id: source.id)
  end
end
