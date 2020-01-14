defmodule LogflareWeb.Source.SearchLVTest do
  use LogflareWeb.ConnCase
  import Phoenix.LiveViewTest
  alias Logflare.Sources
  alias Logflare.Users
  @endpoint LogflareWeb.Endpoint
  import Logflare.Factory
  use Placebo

  setup do
    user = insert(:user)
    source = insert(:source, user: user)
    source = Sources.get(source.id)
    user = Users.get(user.id)
    %{source: [source], user: [user]}
  end

  test "mount", %{conn: conn, source: [s | _], user: [u | _]} do
    conn =
      conn
      |> assign(:user, u)
      |> get("/sources/#{s.id}/search")

    assert html_response(conn, 200) =~ "source-logs-search-container"

    assert {:ok, view, html} = live(conn)
  end

  test "redirected mount", %{conn: conn, source: [s | _], user: [u | _]} do
    assert {:error, %{redirect: %{to: "/"}}} = live(conn, "/sources/1/search")
  end

  test "set_local_time", %{conn: conn, source: [s | _], user: [u | _]} do
    conn =
      conn
      |> assign(:user, u)
      |> get("/sources/#{s.id}/search")

    assert html_response(conn, 200) =~ "source-logs-search-container"
    {:ok, view, html} = live(conn)

    assert render_click(view, "set_local_time", %{"use_local_time" => "true"}) =~
             ~S|id="user-local-timezone"|
  end

  test "user_idle", %{conn: conn, source: [s | _], user: [u | _]} do
    conn =
      conn
      |> assign(:user, u)
      |> get("/sources/#{s.id}/search")

    assert html_response(conn, 200) =~ "source-logs-search-container"
    {:ok, view, html} = live(conn)

    assert render_click(view, "user_idle", %{}) =~
             "Live search paused due to user inactivity."

    refute render_click(view, "remove_flash", %{"flash_key" => "warning"}) =~
             "Live search paused due to user inactivity."
  end

  test "activate_modal/deactivate_modal", %{conn: conn, source: [s | _], user: [u | _]} do
    conn =
      conn
      |> assign(:user, u)
      |> get("/sources/#{s.id}/search")

    assert html_response(conn, 200) =~ "source-logs-search-container"
    {:ok, view, html} = live(conn)

    assert render_click(view, "activate_modal", %{"modal_id" => "searchHelpModal"})
    "Search Your Log Events"

    refute render_click(view, "deactivate_modal", %{}) =~
             "Search Your Log Events"
  end
end
