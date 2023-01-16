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

  test "create/delete webhook", %{conn: conn, source: source} do
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

  test "create/delete google analytics", %{conn: conn, source: source} do
    {:ok, view, _html} =
      live_isolated(conn, SourceBackendsLive, session: %{"source_id" => source.id})

    # create
    assert view
           |> element("button", "Add a backend")
           |> render_click()

    assert view
           |> element("form")
           |> render_change(%{
             _target: ["source_backend", "type"],
             source_backend: %{type: "google_analytics"}
           }) =~ "Measurement ID"

    assert view
           |> element("form")
           |> render_submit(%{
             source_backend: %{
               type: "google_analytics",
               config: %{
                 measurement_id: "G-1234",
                 api_secret: "1234",
                 client_id_path: "metadata.client_id",
                 event_name_paths: "name"
               }
             }
           }) =~ "G-1234"

    #  no form helper text
    refute view |> render() =~ "For authenticating with the GA4 API"

    refute view
           |> element("button", "Remove")
           |> render_click() =~ "G-1234"
  end
end
