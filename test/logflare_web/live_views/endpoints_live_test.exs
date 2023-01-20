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

    {:ok, view, _html} = live(conn, "/endpoints")

    html = view |> element("#endpoints-browser-list") |> render()
    assert html =~ endpoint.name

    render_hook(view, "show-endpoint", %{
      endpoint_id: endpoint.id
    })

    element("#show-endpoint")
    |> assert_patched(view, "/endpoints/#{endpoint.id}")

    html = view |> element("#show-endpoint") |> render()
    assert html =~ endpoint.name
    assert html =~ endpoint.query
  end

  test "show endpoint", %{conn: conn, user: user} do
    endpoint = insert(:endpoint, user: user)

    {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")

    assert view |> element("#show-endpoint") |> render() =~ endpoint.name
    assert view |> element("#show-endpoint") |> render() =~ endpoint.query
  end

  test "show endpoint -> edit endpoint", %{conn: conn, user: user} do
    endpoint = insert(:endpoint, user: user)
    {:ok, view, _html} = live(conn, "/endpoints/#{endpoint.id}")

    render_hook(view, "edit-endpoint", %{endpoint_id: endpoint.id})

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

  test "new endpoint", %{conn: conn} do
    {:ok, view, _html} = live(conn, "/endpoints/new")
    render_hook(view, "new-endpoint")
    assert_patched(view, "/endpoints/new")
    assert view |> has_element?("#new-endpoint")

    # edit the endpoint
    new_query = "select current_timestamp() as my_time"

    render_hook(view, "save-endpoint", %{
      endpoint: %{
        name: "some query",
        query: new_query
      }
    })

    assert view |> element("#show-endpoint") |> render() =~ new_query
  end
end
