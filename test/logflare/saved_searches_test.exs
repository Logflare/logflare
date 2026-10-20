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
    other = insert(:saved_search, source: other_source)
    insert(:saved_search, source: source, favorite: true, saved_by_user: false)
    insert(:saved_search)

    assert Enum.map(SavedSearches.Cache.list_by_sources([source.id]), & &1.id) ==
             [favorite.id, alpha.id, unnamed.id]

    for list <- [&SavedSearches.list_by_sources/1, &SavedSearches.Cache.list_by_sources/1] do
      assert list.([]) == []

      searches = list.([other_source.id, empty_source.id, source.id, source.id])

      assert Enum.map(searches, & &1.id) == [favorite.id, alpha.id, unnamed.id, other.id]
    end
  end

  test "get_by_user_access/2 returns only saved searches on accessible sources", %{
    user: user,
    source: source
  } do
    saved_search = insert(:saved_search, source: source)
    unsaved_search = insert(:saved_search, source: source, saved_by_user: false)
    other_search = insert(:saved_search)

    assert %SavedSearch{id: id} = SavedSearches.get_by_user_access(user, saved_search.id)
    assert id == saved_search.id
    assert %SavedSearch{id: ^id} = SavedSearches.get_by_user_access(user, to_string(id))
    assert SavedSearches.get_by_user_access(user, unsaved_search.id) == nil
    assert SavedSearches.get_by_user_access(user, other_search.id) == nil

    Repo.delete!(saved_search)
    assert SavedSearches.get_by_user_access(user, id) == nil
  end

  test "get_by_user_access/2 supports team members as users and team users", %{
    user: owner,
    source: source
  } do
    team = insert(:team, user: owner)
    team_user = insert(:team_user, team: team)
    member = insert(:user, email: team_user.email)
    saved_search = insert(:saved_search, source: source)
    unsaved_search = insert(:saved_search, source: source, saved_by_user: false)
    other_search = insert(:saved_search)

    for user <- [member, team_user] do
      assert %SavedSearch{id: id} = SavedSearches.get_by_user_access(user, saved_search.id)
      assert id == saved_search.id
      assert SavedSearches.get_by_user_access(user, unsaved_search.id) == nil
      assert SavedSearches.get_by_user_access(user, other_search.id) == nil
    end
  end

  test "save_by_user/4, delete_by_user/1 marks search as saved & unsaved", %{source: source} do
    assert [] == SavedSearches.list_by_sources([source.id])

    # inserts the SavedSearch if does not exist
    assert {:ok, %SavedSearch{saved_by_user: true}} =
             SavedSearches.save_by_user(
               "other-query-string",
               @valid_attrs.lql_rules,
               source,
               @valid_attrs.tailing
             )

    assert [%{querystring: "other-query-string"}] =
             SavedSearches.list_by_sources([source.id])

    # updates the SavedSearch if it exists
    assert {:ok, _saved_search} = SavedSearches.insert(@valid_attrs, source)

    assert {:ok, %SavedSearch{saved_by_user: true} = saved_search} =
             SavedSearches.save_by_user(
               @valid_attrs.querystring,
               @valid_attrs.lql_rules,
               source,
               @valid_attrs.tailing
             )

    assert Enum.map(
             SavedSearches.list_by_sources([source.id]),
             & &1.querystring
           ) ==
             ["other-query-string", @valid_attrs.querystring]

    assert {:ok, %SavedSearch{saved_by_user: false}} = SavedSearches.delete_by_user(saved_search)

    assert [%{querystring: "other-query-string"}] =
             SavedSearches.list_by_sources([source.id])
  end

  test "optional names persist without changing the saved query", %{source: source} do
    assert {:ok, search} = SavedSearches.insert(@valid_attrs, source)
    assert search.name == nil

    name = String.duplicate("n", 255)
    assert {:ok, named} = SavedSearches.update(search, %{name: name})
    assert SavedSearches.get(search.id).name == name
    assert named.querystring == search.querystring

    assert {:error, changeset} = SavedSearches.update(named, %{name: name <> "n", favorite: true})
    assert "should be at most 255 character(s)" in errors_on(changeset).name
    assert SavedSearches.get(search.id).name == name
    refute SavedSearches.get(search.id).favorite

    name = String.duplicate("e\u0301", 127) <> "x"
    assert {:ok, named} = SavedSearches.update(named, %{name: name})
    assert SavedSearches.get(search.id).name == name

    assert {:error, changeset} = SavedSearches.update(named, %{name: name <> "\u0301"})
    assert "should be at most 255 character(s)" in errors_on(changeset).name
    assert SavedSearches.get(search.id).name == name

    assert {:ok, resaved} =
             SavedSearches.save_by_user(search.querystring, [], source, true)

    assert resaved.id == search.id
    assert resaved.name == name
    assert {:ok, unnamed} = SavedSearches.update(resaved, %{name: "   "})
    assert SavedSearches.get(unnamed.id).name == nil
  end

  test "favorites persist and sort first, with the regular order restored after unstarring", %{
    source: source
  } do
    zulu = insert(:saved_search, source: source, querystring: "Alpha", name: "Zulu search")
    alpha = insert(:saved_search, source: source, querystring: "zulu", name: "alpha search")
    insert(:saved_search, source: source, favorite: true, saved_by_user: false)

    assert Enum.map(SavedSearches.list_by_sources([source.id]), & &1.id) ==
             [alpha.id, zulu.id]

    assert {:ok, favorite} =
             SavedSearches.update(zulu, %{"favorite" => true, "name" => "Bravo search"})

    assert SavedSearches.get(zulu.id).favorite
    assert SavedSearches.get(zulu.id).name == "Bravo search"
    assert favorite.querystring == zulu.querystring

    assert Enum.map(SavedSearches.list_by_sources([source.id]), & &1.id) ==
             [zulu.id, alpha.id]

    assert {:ok, _search} = SavedSearches.update(alpha, %{favorite: true})

    assert Enum.map(SavedSearches.list_by_sources([source.id]), & &1.id) ==
             [alpha.id, zulu.id]

    assert {:ok, _search} = SavedSearches.update(alpha, %{favorite: false})
    assert {:ok, _search} = SavedSearches.update(favorite, %{favorite: false})
    refute SavedSearches.get(zulu.id).favorite

    assert Enum.map(SavedSearches.list_by_sources([source.id]), & &1.id) ==
             [alpha.id, zulu.id]

    unnamed = insert(:saved_search, source: source, name: nil, querystring: "ZZZ")
    empty_name = insert(:saved_search, source: source, name: "", querystring: "Beta")

    assert Enum.map(SavedSearches.list_by_sources([source.id]), & &1.id) ==
             [alpha.id, empty_name.id, zulu.id, unnamed.id]
  end

  test "suggest_saved_searches/2", %{source: source} do
    assert {:ok, %SavedSearch{} = saved_search} =
             SavedSearches.insert(%{@valid_attrs | querystring: "sometestsomething"}, source)

    assert [saved_search] == SavedSearches.suggest_saved_searches("test", source.id)
    assert [] == SavedSearches.suggest_saved_searches("other", source.id)
  end
end
