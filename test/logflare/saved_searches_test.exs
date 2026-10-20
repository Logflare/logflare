defmodule Logflare.SavedSearchesTest do
  use Logflare.DataCase
  alias Logflare.{SavedSearches, SavedSearch, Repo}

  setup do
    user = insert(:user)
    source = insert(:source, user_id: user.id)
    [user: user, source: source]
  end

  @valid_attrs %{lql_rules: [], querystring: "testing", saved_by_user: false, tailing: false}
  test "insert/2, get/1, delete/1, get_by_qs_source_id/1", %{source: source} do
    assert {:ok, %SavedSearch{name: "My saved search"} = saved_search} =
             SavedSearches.insert(Map.put(@valid_attrs, :name, "My saved search"), source)

    assert saved_search == SavedSearches.get(saved_search.id)
    assert saved_search == SavedSearches.get_by_qs_source_id(saved_search.querystring, source.id)
    assert {:ok, %SavedSearch{}} = Repo.delete(saved_search)
    assert nil == SavedSearches.get(saved_search.id)
  end

  test "list_by_sources/1 returns saved searches for each source in favorite and name order", %{
    source: source
  } do
    other_source = insert(:source, user_id: source.user_id)
    empty_source = insert(:source, user_id: source.user_id)
    favorite = insert(:saved_search, source: source, name: "Zulu", favorite: true)
    alpha = insert(:saved_search, source: source, name: "alpha")
    unnamed = insert(:saved_search, source: source, name: nil, querystring: "Beta")
    empty_name = insert(:saved_search, source: source, name: "", querystring: "aardvark")
    other = insert(:saved_search, source: other_source)
    insert(:saved_search, source: source, favorite: true, saved_by_user: false)
    insert(:saved_search)

    assert Enum.map(SavedSearches.Cache.list_by_sources([source.id]), & &1.id) ==
             [favorite.id, empty_name.id, alpha.id, unnamed.id]

    for list <- [&SavedSearches.list_by_sources/1, &SavedSearches.Cache.list_by_sources/1] do
      assert list.([]) == []

      searches = list.([other_source.id, empty_source.id, source.id, source.id])

      assert Enum.map(searches, & &1.id) ==
               [favorite.id, empty_name.id, alpha.id, unnamed.id, other.id]
    end
  end

  test "get_by_user_access/2 returns only saved searches on accessible sources", %{
    user: owner,
    source: source
  } do
    team = insert(:team, user: owner)
    team_user = insert(:team_user, team: team)
    member = insert(:user, email: team_user.email)
    %{id: id} = saved_search = insert(:saved_search, source: source)
    unsaved_search = insert(:saved_search, source: source, saved_by_user: false)
    other_search = insert(:saved_search)

    for actor <- [owner, member, team_user] do
      assert %SavedSearch{id: ^id} = SavedSearches.get_by_user_access(actor, id)
      assert %SavedSearch{id: ^id} = SavedSearches.get_by_user_access(actor, to_string(id))
      assert SavedSearches.get_by_user_access(actor, unsaved_search.id) == nil
      assert SavedSearches.get_by_user_access(actor, other_search.id) == nil
    end

    Repo.delete!(saved_search)

    for actor <- [owner, member, team_user] do
      assert SavedSearches.get_by_user_access(actor, id) == nil
    end
  end

  test "save_by_user/4 and update/2 mark search as saved & unsaved", %{source: source} do
    assert {:ok, %SavedSearch{saved_by_user: true}} =
             SavedSearches.save_by_user("other-query-string", [], source, false)

    assert {:ok, %{id: id}} = SavedSearches.insert(@valid_attrs, source)

    assert {:ok, %SavedSearch{id: ^id, saved_by_user: true} = saved_search} =
             SavedSearches.save_by_user(@valid_attrs.querystring, [], source, false)

    assert {:ok, %SavedSearch{saved_by_user: false}} =
             SavedSearches.update(saved_search, %{saved_by_user: false})
  end

  test "names use a codepoint limit and persist with favorites when resaved", %{source: source} do
    assert {:ok, search} = SavedSearches.insert(@valid_attrs, source)
    assert search.name == nil

    name = String.duplicate("e\u0301", 127) <> "x"
    assert {:ok, named} = SavedSearches.update(search, %{"name" => name, "favorite" => true})
    assert named.querystring == search.querystring

    assert {:error, changeset} =
             SavedSearches.update(named, %{name: name <> "\u0301", favorite: false})

    assert "should be at most 255 character(s)" in errors_on(changeset).name

    id = search.id

    assert {:ok, %SavedSearch{id: ^id, name: ^name, favorite: true} = resaved} =
             SavedSearches.save_by_user(search.querystring, [], source, true)

    assert {:ok, %SavedSearch{favorite: false}} =
             SavedSearches.update(resaved, %{favorite: false})
  end

  test "suggest_saved_searches/2", %{source: source} do
    assert {:ok, %SavedSearch{} = saved_search} =
             SavedSearches.insert(%{@valid_attrs | querystring: "sometestsomething"}, source)

    assert [saved_search] == SavedSearches.suggest_saved_searches("test", source.id)
    assert [] == SavedSearches.suggest_saved_searches("other", source.id)
  end
end
