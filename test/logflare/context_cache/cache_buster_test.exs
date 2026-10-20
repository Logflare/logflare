defmodule Logflare.ContextCache.CacheBusterTest do
  use Logflare.DataCase

  alias Cainophile.Changes.DeletedRecord
  alias Cainophile.Changes.NewRecord
  alias Cainophile.Changes.Transaction
  alias Cainophile.Changes.UpdatedRecord
  alias Logflare.ContextCache
  alias Logflare.ContextCache.CacheBuster
  alias Logflare.SavedSearches
  alias Logflare.Sources

  setup do
    for child_spec <- ContextCache.Supervisor.buster_specs() do
      start_supervised!(child_spec)
    end

    insert(:plan, name: "Free")
    user = insert(:user)
    source = insert(:source, user: user)
    [source: source, user: user]
  end

  test "cache buster", %{source: %{id: source_id, token: source_token}} do
    Sources.Cache.get_by(token: source_token)
    assert Cachex.size!(Sources.Cache) == 1

    change = %DeletedRecord{
      relation: {"public", "sources"},
      old_record: %{"id" => Integer.to_string(source_id)}
    }

    test_pid = self()

    Mimic.expect(ContextCache, :bust_keys, fn arg ->
      Mimic.call_original(ContextCache, :bust_keys, [arg])
      send(test_pid, arg)
    end)

    send(CacheBuster, %Transaction{changes: [change]})
    assert_receive [{Sources, ^source_id}], 500
    assert Cachex.size!(Sources.Cache) == 0
  end

  test "WAL records invalidate cached saved searches after inserting, updating and unsaving", %{
    source: source
  } do
    other_source = insert(:source, user_id: source.user_id)
    source_ids = [other_source.id, source.id]
    record = %{"source_id" => to_string(source.id)}

    assert SavedSearches.Cache.list_by_sources(source_ids) == []

    assert {:ok, search} =
             SavedSearches.insert(
               %{lql_rules: [], querystring: "testing", name: "Before", saved_by_user: true},
               source
             )

    assert SavedSearches.Cache.list_by_sources(source_ids) == []

    send(CacheBuster, %Transaction{
      changes: [%NewRecord{relation: {"public", "saved_searches"}, record: record}]
    })

    id = search.id

    TestUtils.retry_assert(fn ->
      assert [%{id: ^id, name: "Before", favorite: false}] =
               SavedSearches.Cache.list_by_sources(source_ids)
    end)

    assert {:ok, updated} = SavedSearches.update(search, %{name: "After", favorite: true})

    assert [%{name: "Before", favorite: false}] = SavedSearches.Cache.list_by_sources(source_ids)

    send(CacheBuster, %Transaction{
      changes: [%UpdatedRecord{relation: {"public", "saved_searches"}, record: record}]
    })

    TestUtils.retry_assert(fn ->
      assert [%{id: ^id, name: "After", favorite: true}] =
               SavedSearches.Cache.list_by_sources(source_ids)
    end)

    assert {:ok, _unsaved} = SavedSearches.delete_by_user(updated)
    assert [%{id: ^id}] = SavedSearches.Cache.list_by_sources(source_ids)

    send(CacheBuster, %Transaction{
      changes: [%UpdatedRecord{relation: {"public", "saved_searches"}, record: record}]
    })

    TestUtils.retry_assert(fn ->
      assert SavedSearches.Cache.list_by_sources(source_ids) == []
    end)
  end
end
