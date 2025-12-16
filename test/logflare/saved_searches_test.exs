defmodule Logflare.SavedSearchesTest do
  use Logflare.DataCase
  alias Logflare.{SavedSearches, SavedSearch}

  setup do
    user = insert(:user)
    source = insert(:source, user_id: user.id)
    [user: user, source: source]
  end

  @valid_attrs %{lql_rules: [], querystring: "testing", saved_by_user: false, tailing: false}
  test "insert/2, get/1, delete/1, get_by_qs_source_id/1", %{source: source} do
    assert {:ok, %SavedSearch{} = saved_search} = SavedSearches.insert(@valid_attrs, source)
    assert saved_search == SavedSearches.get(saved_search.id)
    assert saved_search == SavedSearches.get_by_qs_source_id(saved_search.querystring, source.id)
    assert {:ok, %SavedSearch{}} = SavedSearches.delete(saved_search)
    assert nil == SavedSearches.get(saved_search.id)
  end

  test "save_by_user/4, delete_by_user/1 marks search as saved & unsaved", %{source: source} do
    # inserts the SavedSearch if does not exist
    assert {:ok, %SavedSearch{saved_by_user: true}} =
             SavedSearches.save_by_user(
               "other-query-string",
               @valid_attrs.lql_rules,
               source,
               @valid_attrs.tailing
             )

    # updates the SavedSearch if it exists
    assert {:ok, saved_search} = SavedSearches.insert(@valid_attrs, source)

    assert {:ok, %SavedSearch{saved_by_user: true}} =
             SavedSearches.save_by_user(
               @valid_attrs.querystring,
               @valid_attrs.lql_rules,
               source,
               @valid_attrs.tailing
             )

    assert {:ok, %SavedSearch{saved_by_user: false}} = SavedSearches.delete_by_user(saved_search)
  end

  test "suggest_saved_searches/2", %{source: source} do
    assert {:ok, %SavedSearch{} = saved_search} =
             SavedSearches.insert(%{@valid_attrs | querystring: "sometestsomething"}, source)

    assert [saved_search] == SavedSearches.suggest_saved_searches("test", source.id)
    assert [] == SavedSearches.suggest_saved_searches("other", source.id)
  end

  test "list_saved_searches_by_user/1", %{user: user} do
    other_user = insert(:user)

    # Create sources with sortable names
    source_b = insert(:source, user: user, name: "b-source")
    source_a = insert(:source, user: user, name: "a-source")
    source_c = insert(:source, user: user, name: "c-source")
    other_source = insert(:source, user: other_user, name: "other-source")

    {:ok, _search_b1} =
      SavedSearches.insert(%{@valid_attrs | saved_by_user: true, querystring: "b1"}, source_b)

    {:ok, _search_a1} =
      SavedSearches.insert(%{@valid_attrs | saved_by_user: true, querystring: "a1"}, source_a)

    {:ok, _search_a2} =
      SavedSearches.insert(%{@valid_attrs | saved_by_user: true, querystring: "a2"}, source_a)

    {:ok, _search_c1} =
      SavedSearches.insert(%{@valid_attrs | saved_by_user: true, querystring: "c1"}, source_c)

    # not saved by user: should not be returned
    {:ok, _not_saved} =
      SavedSearches.insert(
        %{@valid_attrs | saved_by_user: false, querystring: "not-saved"},
        source_a
      )

    # not owned by user: should not be returned
    {:ok, _other_search} =
      SavedSearches.insert(
        %{@valid_attrs | saved_by_user: true, querystring: "other"},
        other_source
      )

    result = SavedSearches.list_saved_searches_by_user(user.id)

    assert ["a1", "a2", "b1", "c1"] == result |> Enum.map(& &1.querystring)
  end
end
