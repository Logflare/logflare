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

  test "index", %{conn: conn, user: user, source: source} do
    backend = insert(:backend, sources: [source], user: user)
    {:ok, view, _html} = live(conn, ~p"/backends")

    html = render(view)
    assert html =~ "rules: 0"
    assert html =~ backend.name
  end

  test "show", %{conn: conn, user: user, source: source} do
    backend = insert(:backend, sources: [source], user: user)
    {:ok, view, _html} = live(conn, ~p"/backends/#{backend.id}")

    html = render(view)
    assert html =~ backend.name
    assert html =~ "#{backend.type}"
  end

  test "edit", %{conn: conn, user: user, source: source} do
    backend = insert(:backend, sources: [source], user: user)
    {:ok, view, _html} = live(conn, ~p"/backends/#{backend.id}/edit")

    assert view |> element("label", "Name") |> has_element?()
  end

  test "bug: string user_id on session for team users", %{conn: conn, user: user} do
    conn = put_session(conn, :user_id, inspect(user.id))
    assert {:ok, _view, _html} = live(conn, ~p"/backends")
  end

  test "create", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/backends")

    # create
    assert view
           |> element("a", "New backend")
           |> render_click() =~ ~r/\/new/

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
    assert render(view) =~ "Successfully created backend"
  end

  test "delete", %{
    conn: conn,
    user: user
  } do
    backend =
      insert(:backend,
        sources: [],
        user: user,
        name: "my webhook",
        type: "webhook",
        config: %{
          url: "http://localhost:1234"
        }
      )

    {:ok, view, _html} = live(conn, ~p"/backends/#{backend.id}/edit")

    refute view
           |> element("button", "Delete")
           |> render_click() =~ "localhost"

    assert_patched(view, ~p"/backends")
    refute render(view) =~ "my webhook"
    assert render(view) =~ "Successfully deleted backend"
  end

  test "error on deleting a backend with attached sources", %{
    conn: conn,
    user: user,
    source: source
  } do
    backend = insert(:backend, sources: [source], user: user)
    {:ok, view, _html} = live(conn, ~p"/backends/#{backend.id}/edit")

    assert view
           |> element("button", "Delete")
           |> render_click() =~ backend.name

    assert view
           |> render() =~ "There are still sources connected to this backend"
  end

  test "on backend type switch, will change the inputs", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/backends/new")

    refute render(view) =~ "Postgres URL"

    assert view
           |> element("select#type")
           |> render_change(%{backend: %{type: "postgres"}}) =~ "Postgres URL"
  end

  test "index: able to view number of source rules attached", %{
    conn: conn,
    user: user,
    source: source
  } do
    rule = insert(:rule, source: source)
    backend = insert(:backend, rules: [rule], user: user)
    {:ok, view, _html} = live(conn, ~p"/backends")

    assert render(view) =~ "rules: 1"

    html =
      view
      |> element("a", backend.name)
      |> render_click()

    assert html =~ rule.lql_string
    assert html =~ rule.source.name
  end

  test "show: add/delete a rule", %{
    conn: conn,
    user: user,
    source: source
  } do
    backend = insert(:backend, rules: [], user: user)
    {:ok, view, _html} = live(conn, ~p"/backends/#{backend.id}")

    assert view
           |> element("button", "Add a rule")
           |> render_click() =~ "Source"

    html =
      view
      |> element("form#rule")
      |> render_submit(%{
        rule: %{
          lql_string: "my:value",
          source_id: source.id,
          backend_id: backend.id
        }
      })

    assert html =~ "Successfully created rule for #{backend.name}"
    assert html =~ "my:value"
    assert html =~ source.name

    html =
      view
      |> element("ul li button", "Delete rule")
      |> render_click()

    refute html =~ "my:value"
    refute html =~ source.name
  end
end
