defmodule LogflareWeb.AccessTokensLiveTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  setup %{conn: conn} do
    insert(:plan)
    user = insert(:user)
    conn = conn |> put_session(:user_id, user.id) |> assign(:user, user)

    {:ok, user: user, conn: conn}
  end

  test "subheader", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/access-tokens")

    assert view
           |> element("a", "docs")
           |> has_element?()
  end

  test "legacy api key - show only when no access tokens", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/access-tokens")
    html = render(view)
    # able to copy, visible
    assert view
           |> element("button", "Copy")
           |> has_element?()

    # able to see legacy user token
    assert html =~ "Deprecated"
    assert html =~ "Copy"
  end

  test "deprecated: public token", %{conn: conn, user: user} do
    token = insert(:access_token, scopes: "public", resource_owner: user)
    {:ok, view, _html} = live(conn, ~p"/access-tokens")
    html = render(view)
    # able to copy, visible
    assert view
           |> element("button", "Copy")
           |> has_element?()

    assert html =~ token.token
    assert html =~ "public"
    refute html =~ "Deprecated"
    assert html =~ "No description"
  end

  test "create token - ingest into any", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/access-tokens")
    html = do_ui_create_token(view, "ingest")
    html = render(view)
    # able to copy, visible
    assert view
           |> element("button", "Copy")
           |> has_element?()

    assert html =~ "ingest"
  end

  test "create token - ingest into one source", %{conn: conn, user: user} do
    source = insert(:source, user: user)
    {:ok, view, _html} = live(conn, ~p"/access-tokens")
    html = do_ui_create_token(view, "ingest:source:#{source.id}")
    # able to copy, visible
    assert view
           |> element("button", "Copy")
           |> has_element?()

    assert html =~ "ingest for #{source.name}"
  end

  test "create token - query for one endpoint", %{conn: conn, user: user} do
    endpoint = insert(:endpoint, user: user)
    {:ok, view, _html} = live(conn, ~p"/access-tokens")
    html = do_ui_create_token(view, "query:endpoint:#{endpoint.id}")

    assert view
           |> element("button", "Copy")
           |> has_element?()

    assert html =~ "query for #{endpoint.name}"
  end

  test "create ingest token", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/access-tokens")

    html = do_ui_create_token(view, "ingest")

    assert view
           |> element("button", "Copy")
           |> has_element?()

    assert html =~ "some description"
    assert html =~ "ingest"
  end

  test "create private token", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/access-tokens")

    assert view
           |> element("button", "Create access token")
           |> render_click()

    assert view |> element("button", "Create") |> has_element?()
    assert view |> element("label", "Scope") |> has_element?()

    assert view
           |> element("form")
           |> render_submit(%{
             description: "some description",
             scopes: "private"
           }) =~ "created successfully"

    html = view |> element("table") |> render()
    assert html =~ "some description"
    assert html =~ "private"
  end

  test "show private token", %{conn: conn, user: user} do
    token = insert(:access_token, scopes: "private", resource_owner: user)
    {:ok, view, _html} = live(conn, ~p"/access-tokens")

    # not able to copy, not visible
    refute view
           |> element("button", "Copy")
           |> has_element?()

    html = render(view)
    refute html =~ token.token
    assert html =~ "private"
  end

  # returns the rendered table html
  defp do_ui_create_token(view, scopes) do
    assert view
           |> element("button", "Create access token")
           |> render_click()

    assert view |> element("button", "Create") |> has_element?()
    assert view |> element("label", "Scope") |> has_element?()

    assert view
           |> element("form")
           |> render_submit(%{
             description: "some description",
             scopes: scopes
           }) =~ "created successfully"

    view |> element("table") |> render()
  end
end
