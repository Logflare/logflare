defmodule LogflareWeb.SourceBackendsLiveTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  import Phoenix.LiveViewTest
  alias LogflareWeb.SourceBackendsLive

  setup do
    insert(:plan)
    user = insert(:user)
    source = insert(:source, user_id: user.id)

    [source: source, user: user]
  end

  test "able to add/remove additional backends", %{conn: conn, user: user, source: source} do
    backend = insert(:postgres_backend, user: user)

    {:ok, view, _html} =
      live_isolated(conn, SourceBackendsLive, session: %{"source_id" => source.id})

    # create
    assert render(view) =~ "PostgreSQL"
    assert render(view) =~ "BigQuery"
    assert render(view) =~ backend.name
    assert render(view) =~ "Save"

    assert view
           |> element("form")
           |> render_submit(%{
             source: %{
               backends: [backend.id]
             }
           }) =~ "connected: 1"

    #  remove
    refute view
           |> element("form")
           |> render_submit(%{
             source: %{
               backends: [backend.id]
             }
           }) =~ "connected: 0"
  end
end
