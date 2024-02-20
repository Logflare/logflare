defmodule LogflareWeb.BackendsLiveTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  import Phoenix.LiveViewTest
  alias LogflareWeb.BackendsLive

  setup do
    user = insert(:user)
    source = insert(:source, user_id: user.id)
    {:ok, source: source}
  end

  test "create/delete", %{conn: conn, source: source} do
    {:ok, view, _html} =
      live_isolated(conn, BackendsLive, session: %{"source_id" => source.id})

    # create
    assert view
           |> element("button", "Add a backend")
           |> render_click() =~ "Add backend"

    assert view
           |> element("form")
           |> render_submit(%{backend_form: %{type: "webhook", url: "http://localhost:1234"}}) =~
             "localhost"

    refute render(view) =~ "URL"

    refute view
           |> element("button", "Remove")
           |> render_click() =~ "localhost"
  end
end
