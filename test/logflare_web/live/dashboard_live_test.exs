defmodule LogflareWeb.DashboardLiveTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  alias Logflare.Repo

  setup %{conn: conn} do
    insert(:plan)
    user = insert(:user)
    team = insert(:team, user: user)
    source = insert(:source, user: user)
    user = %{user | team: team}
    conn = conn |> put_session(:user_id, user.id) |> assign(:user, user)

    {:ok, user: user, source: source, conn: conn}
  end

  describe "Dashboard Live" do
    test "renders dashboard", %{conn: conn} do
      {:ok, view, html} = live(conn, "/dashboard_new")

      assert view |> has_element?("h5", "~/logs")
    end

    test "show source", %{conn: conn, source: source} do
      {:ok, view, html} = live(conn, "/dashboard_new")
      assert html =~ source.name
    end
  end

  describe "favoriting a source" do
    test "favorite a source", %{conn: conn, source: source} do
      {:ok, view, _} = live(conn, "/dashboard_new")

      refute source.favorite
      assert view |> element(".favorite .far") |> has_element?()

      view
      |> element("##{source.token} .favorite")
      |> render_click()

      updated_source = source |> Repo.reload()
      assert updated_source.favorite

      assert view |> element(".favorite .fas") |> has_element?()
    end

    test "unfavorite a source", %{conn: conn, source: source} do
      {:ok, favorited_source} = source |> Logflare.Sources.update_source(%{favorite: true})

      {:ok, view, _html} = live(conn, "/dashboard_new")

      assert favorited_source.favorite
      assert view |> element(".favorite .fas") |> has_element?()

      view
      |> element(".favorite")
      |> render_click()

      updated_source = favorited_source |> Repo.reload()
      refute updated_source.favorite
      assert view |> has_element?(".favorite .far")
    end
  end

  describe "saved searches" do
    setup %{source: source} do
      {:ok, saved_search} =
        Logflare.SavedSearches.insert(
          %{
            lql_rules: [],
            querystring: "test query",
            saved_by_user: true,
            tailing: true
          },
          source
        )

      [saved_search: saved_search]
    end

    test "renders saved searches", %{conn: conn, source: source, saved_search: saved_search} do
      {:ok, view, html} = live(conn, "/dashboard_new")

      assert html =~ "Saved Searches"
      assert html =~ "test query"
      assert html =~ source.name
    end
  end
end
