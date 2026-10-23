defmodule Logflare.ContextCache.CacheBusterTest do
  use Logflare.DataCase

  alias Cainophile.Changes.DeletedRecord
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

  test "WAL records invalidate cached saved searches for the source", %{
    source: source
  } do
    %{id: id} = search = insert(:saved_search, source: source, name: "Before")
    assert [%{id: ^id, name: "Before"}] = SavedSearches.Cache.list_by_sources([source.id])
    assert {:ok, _updated} = SavedSearches.update(search, %{name: "After"})
    assert [%{name: "Before"}] = SavedSearches.Cache.list_by_sources([source.id])

    send(CacheBuster, %Transaction{
      changes: [
        %UpdatedRecord{
          relation: {"public", "saved_searches"},
          record: %{"source_id" => to_string(source.id)}
        }
      ]
    })

    TestUtils.retry_assert(fn ->
      assert [%{id: ^id, name: "After"}] = SavedSearches.Cache.list_by_sources([source.id])
    end)
  end
end
