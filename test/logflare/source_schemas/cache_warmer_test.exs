defmodule Logflare.SourceSchemas.CacheWarmerTest do
  use Logflare.DataCase, async: false

  alias Logflare.Repo
  alias Logflare.SourceSchemas.Cache
  alias Logflare.SourceSchemas.CacheWarmer

  setup do
    user = insert(:user)
    source = insert(:source, user: user, log_events_updated_at: NaiveDateTime.utc_now())
    source_schema = insert(:source_schema, source: source)
    Cachex.clear!(Cache)

    {:ok, source: source, source_schema: source_schema}
  end

  test "warms entries under the key the read path uses", %{
    source: source,
    source_schema: source_schema
  } do
    schema_id = source_schema.id

    assert %{id: ^schema_id} = Cache.get_source_schema_by(source_id: source.id)
    assert {:ok, [read_key]} = Cachex.keys(Cache)

    assert {:ok, pairs} = CacheWarmer.execute(nil)
    Cachex.clear!(Cache)
    assert {:ok, true} = Cachex.put_many(Cache, pairs)

    assert {:ok, {:cached, %{id: ^schema_id}}} = Cachex.get(Cache, read_key)
  end

  test "warmed entries are served without hitting the database", %{
    source: source,
    source_schema: source_schema
  } do
    schema_id = source_schema.id

    assert {:ok, pairs} = CacheWarmer.execute(nil)
    assert {:ok, true} = Cachex.put_many(Cache, pairs)
    assert %{} = Repo.delete!(source_schema)

    assert %{id: ^schema_id} = Cache.get_source_schema_by(source_id: source.id)
  end
end
