defmodule LogflareWeb.BackendsLiveTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  import Phoenix.LiveViewTest

  setup %{conn: conn} do
    insert(:plan)
    user = insert(:user)
    source = insert(:source, user_id: user.id)
    conn = login_user(conn, user)

    [conn: conn, source: source, user: user]
  end

  test "bug: string user_id on session for team users", %{conn: conn, user: user} do
    conn = put_session(conn, :user_id, inspect(user.id))
    assert {:ok, _view, _html} = live(conn, ~p"/backends")
  end

  test "create/delete", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/backends")

    # create
    assert view
           |> element("button", "Add a backend")
           |> render_click() =~ "Add backend"

    assert view
           |> element("form")
           |> render_submit(%{
             backend: %{
               name: "my webhook",
               type: "webhook",
               config: %{
                 url: "http://localhost:1234"
               }
             }
           }) =~
             "localhost"

    refute render(view) =~ "URL"

    assert render(view) =~ "my webhook"

    refute view
           |> element("button", "Remove")
           |> render_click() =~ "localhost"

    refute render(view) =~ "my webhook"
  end

  test "error on deleting a backend with attached sources", %{
    conn: conn,
    user: user,
    source: source
  } do
    backend = insert(:backend, sources: [source], user: user)
    {:ok, view, _html} = live(conn, ~p"/backends")

    assert view
           |> element("button", "Remove")
           |> render_click() =~ backend.name

    assert view
           |> render() =~ "There are still sources connected to this backend"
  end

  test "on backend type switch, will change the inputs", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/backends")

    view
    |> element("button", "Add a backend")
    |> render_click()

    refute render(view) =~ "Postgres URL"

    assert view
           |> element("select#type")
           |> render_change(%{backend: %{type: "postgres"}}) =~ "Postgres URL"
  end
end
