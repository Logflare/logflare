defmodule LogflareWeb.EndpointsLiveTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  setup %{conn: conn} do
    insert(:plan)
    user = insert(:user)

    conn =
      conn
      |> login_user(user)

    {:ok, user: user, conn: conn}
  end

  test "list endpoints", %{conn: conn, user: user} do
    endpoint = insert(:endpoint, user: user)

    {:ok, view, _html} =
      conn
      |> login_user(user)
      |> live("/endpoints")

    assert render(view) =~ endpoint.name
    # navigate to an endpoint
    html =
      view
      |> element("a", endpoint.name)
      |> render_click()

    assert_patched(view, "/endpoints/#{endpoint.id}")
    assert html =~ endpoint.name
    assert html =~ endpoint.query
  end

  test "show endpoint", %{conn: conn, user: user} do
    endpoint = insert(:endpoint, user: user)

    {:ok, view, _html} =
      conn
      |> login_user(user)
      |> live("/endpoints/#{endpoint.id}")

    assert render(view) =~ endpoint.name
    assert render(view) =~ endpoint.query

    # Static elements
    assert render(view) =~ "Edit Query"
  end

  test "show endpoint -> edit endpoint", %{conn: conn, user: user} do
    endpoint = insert(:endpoint, user: user)
    {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")

    view
    |> element("button", "Edit Query")
    |> render_click()

    assert_patched(view, "/endpoints/#{endpoint.id}/edit")

    assert view |> has_element?("#edit-endpoint")
    assert view |> element("#edit-endpoint") |> render() =~ endpoint.name

    # edit the endpoint
    new_query = "select current_timestamp() as my_time"

    render_hook(view, "save-endpoint", %{
      endpoint: %{
        query: new_query
      }
    })

    assert view |> render() =~ new_query
  end

  @tag skip: true
  test "new endpoint" do
  end
end
