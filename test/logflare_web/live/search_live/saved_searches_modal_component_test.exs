defmodule LogflareWeb.SearchLive.SavedSearchesModalComponentTest do
  use LogflareWeb.ConnCase, async: false

  alias Logflare.SavedSearches
  alias LogflareWeb.SavedSearchesModalLive

  setup do
    user = insert(:user)
    [user: user, source: insert(:source, user: user)]
  end

  test "shows the empty state and favoriting guidance", %{conn: conn, user: user, source: source} do
    view = open_modal(conn, user, source)

    assert has_element?(view, "#saved-searches-empty", "no saved searches")
    assert has_element?(view, "#saved-searches-modal", "Star a saved search")
  end

  test "displays saved search names, descriptions, queries, and the untitled fallback", %{
    conn: conn,
    user: user,
    source: source
  } do
    named =
      insert(:saved_search,
        source: source,
        name: "Errors by status",
        description: "Investigate server errors"
      )

    unnamed = insert(:saved_search, source: source)

    view = open_modal(conn, user, source)

    assert has_element?(view, "#run-saved-search-#{named.id}", "Errors by status")

    assert has_element?(
             view,
             "#saved-search-description-#{named.id}",
             "Investigate server errors"
           )

    assert has_element?(view, "#run-saved-search-#{named.id} code", named.querystring)
    assert has_element?(view, "#copy-saved-search-#{named.id}")
    assert has_element?(view, "#run-saved-search-#{unnamed.id}", "untitled")
    assert has_element?(view, "#run-saved-search-#{unnamed.id} code", unnamed.querystring)
  end

  test "rejects a forged source ID even when its saved searches are cached", %{
    conn: conn,
    user: user,
    source: source
  } do
    own_search = insert(:saved_search, source: source, name: "My search")
    foreign_source = insert(:source, user: insert(:user))
    foreign_search = insert(:saved_search, source: foreign_source, name: "Private search")

    SavedSearches.Cache.list_by_sources([foreign_source.id])

    view = open_modal(conn, user, foreign_source)

    assert has_element?(view, "#saved-searches-modal", "Failed to load saved searches")
    refute render(view) =~ foreign_search.querystring

    for {event, attrs} <- [
          {"edit_name", %{}},
          {"update_saved_search",
           %{"saved_by_user" => false, "favorite" => true, "name" => "Forbidden rename"}}
        ],
        search <- [own_search, foreign_search] do
      view
      |> with_target("#saved-searches-modal")
      |> render_click(event, %{"id" => to_string(search.id), "saved_search" => attrs})
    end

    for search <- [own_search, foreign_search] do
      assert %{saved_by_user: true, favorite: false, name: name} = SavedSearches.get(search.id)
      assert name == search.name
    end

    refute has_element?(view, "#saved-searches-list form")
  end

  test "untitled searches cannot be starred until a title is added", %{
    conn: conn,
    user: user,
    source: source
  } do
    search = insert(:saved_search, source: source)

    view = open_modal(conn, user, source)

    assert has_element?(view, "#favorite-saved-search-#{search.id}[disabled] i.far.fa-star")

    assert has_element?(
             view,
             "#favorite-saved-search-tooltip-#{search.id}[data-toggle='tooltip'][data-original-title='Add a title before favouriting this search.']"
           )

    view |> element("#edit-saved-search-name-#{search.id}") |> render_click()

    view
    |> form("#rename-saved-search-#{search.id}", saved_search: %{name: "Newly titled"})
    |> render_submit()

    refute has_element?(view, "#favorite-saved-search-#{search.id}[disabled]")
    view |> element("#favorite-saved-search-#{search.id}") |> render_click()

    assert has_element?(view, "#favorite-saved-search-#{search.id}[aria-pressed='true']")
    assert SavedSearches.get(search.id).favorite
  end

  test "clearing a favorite's title shows a validation error without changing its saved name", %{
    conn: conn,
    user: user,
    source: source
  } do
    search = insert(:saved_search, source: source, name: "Production errors", favorite: true)

    view = open_modal(conn, user, source)
    view |> element("#edit-saved-search-name-#{search.id}") |> render_click()

    view
    |> form("#rename-saved-search-#{search.id}", saved_search: %{name: ""})
    |> render_submit()

    assert has_element?(
             view,
             "#rename-saved-search-#{search.id}",
             "Can’t be blank when a favorite"
           )

    assert %{name: "Production errors", favorite: true} = SavedSearches.get(search.id)
  end

  describe "initial naming" do
    setup %{conn: conn, user: user, source: source} do
      first = insert(:saved_search, source: source, name: "Alpha", favorite: true)
      last = insert(:saved_search, source: source, name: "Beta")
      search = insert(:saved_search, source: source)

      view = open_modal(conn, user, source, %{"edit-search-id" => to_string(search.id)})

      [view: view, first: first, last: last, search: search]
    end

    test "pins only the selected search and restores ordering on cancel", %{
      view: view,
      first: first,
      last: last,
      search: search
    } do
      assert has_element?(view, "#saved-searches-list > li:first-child#saved-search-#{search.id}")
      assert has_element?(view, "#rename-saved-search-#{search.id}")
      refute has_element?(view, "#rename-saved-search-#{first.id}")
      refute has_element?(view, "#rename-saved-search-#{last.id}")

      view |> element("#cancel-saved-search-name-#{search.id}") |> render_click()

      refute has_element?(view, "#saved-searches-list form")
      assert has_element?(view, "#saved-searches-list > li:first-child#saved-search-#{first.id}")
      assert has_element?(view, "#saved-searches-list > li:nth-child(2)#saved-search-#{last.id}")
      assert has_element?(view, "#saved-searches-list > li:last-child#saved-search-#{search.id}")
    end

    test "keeps invalid initial naming pinned, then restores ordering on save and normal edits",
         %{
           view: view,
           first: first,
           last: last,
           search: search
         } do
      view
      |> form("#rename-saved-search-#{search.id}",
        saved_search: %{name: String.duplicate("x", 256)}
      )
      |> render_submit()

      assert has_element?(view, "#rename-saved-search-#{search.id}")
      assert has_element?(view, "#saved-searches-list > li:first-child#saved-search-#{search.id}")

      view
      |> form("#rename-saved-search-#{search.id}", saved_search: %{name: "Aardvark"})
      |> render_submit()

      refute has_element?(view, "#saved-searches-list form")
      assert has_element?(view, "#saved-searches-list > li:first-child#saved-search-#{first.id}")

      assert has_element?(
               view,
               "#saved-searches-list > li:nth-child(2)#saved-search-#{search.id}"
             )

      assert has_element?(view, "#saved-searches-list > li:last-child#saved-search-#{last.id}")

      view |> element("#edit-saved-search-name-#{search.id}") |> render_click()

      assert has_element?(view, "#rename-saved-search-#{search.id}")

      assert has_element?(
               view,
               "#saved-searches-list > li:nth-child(2)#saved-search-#{search.id}"
             )
    end
  end

  test "edits retain invalid input and persist corrected values", %{
    conn: conn,
    user: user,
    source: source
  } do
    search =
      insert(:saved_search,
        source: source,
        name: "Original errors",
        description: "Original context",
        tailing: false
      )

    description = "Errors in production\nExclude health checks."

    view = open_modal(conn, user, source)
    view |> element("#edit-saved-search-name-#{search.id}") |> render_click()

    assert has_element?(view, "#saved-search-name-#{search.id}[value='Original errors']")
    assert has_element?(view, "#saved-search-description-#{search.id}", "Original context")

    view
    |> form("#rename-saved-search-#{search.id}",
      saved_search: %{name: String.duplicate("x", 256), description: description}
    )
    |> render_submit()

    assert has_element?(view, "#rename-saved-search-#{search.id}", "should be at most 255")
    assert has_element?(view, "#saved-search-description-#{search.id}", description)

    assert %{name: "Original errors", description: "Original context"} =
             SavedSearches.get(search.id)

    view
    |> form("#rename-saved-search-#{search.id}",
      saved_search: %{name: "Production errors", description: description}
    )
    |> render_submit()

    refute has_element?(view, "#rename-saved-search-#{search.id}")
    assert has_element?(view, "#run-saved-search-#{search.id}", "Production errors")
    assert has_element?(view, "#saved-search-description-#{search.id}", description)

    assert %{name: "Production errors", description: ^description, tailing: false} =
             updated = SavedSearches.get(search.id)

    assert updated.querystring == search.querystring
  end

  @spec open_modal(Plug.Conn.t(), map(), map(), map()) :: Phoenix.LiveViewTest.View.t()
  defp open_modal(conn, user, source, params \\ %{}) do
    params = Map.put(params, "source-id", to_string(source.id))

    {:ok, view, _html} =
      live_isolated(conn, SavedSearchesModalLive, session: %{"user" => user, "params" => params})

    render_async(view)
    view
  end
end
