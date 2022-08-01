defmodule LogflareWeb.SourceBackendsLiveTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  import Phoenix.LiveViewTest
  alias LogflareWeb.SourceBackendsLive

  setup do
    user = insert(:user)
    source = insert(:source, user_id: user.id)
    {:ok, source: source}
  end

  test "create/delete", %{conn: conn, source: source} do
    {:ok, view, _html} =
      live_isolated(conn, SourceBackendsLive, session: %{"source_id" => source.id})

    # create
    assert view
           |> element("button", "Add a backend")
           |> render_click() =~ "Url"

    assert view
           |> element("form")
           |> render_submit(%{
             source_backend: %{
               type: "webhook",
               config: %{url: "http://localhost:1234"}
             }
           }) =~ "localhost"

    refute view |> render() =~ "Url"

    refute view
           |> element("button", "Remove")
           |> render_click() =~ "localhost"
  end
end
