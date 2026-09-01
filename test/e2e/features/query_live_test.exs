defmodule E2e.Features.QueryLiveTest do
  use Logflare.FeatureCase, async: false

  import LogflareWeb.MonacoEditorTestUtils

  describe "query page Monaco editor" do
    TestUtils.setup_single_tenant(seed_user: true, backend_type: :postgres)

    test "runs the query entered in Monaco", %{conn: conn} do
      query = "select 42 as answer, 'monaco e2e result' as label"

      conn =
        conn
        |> visit(~p"/auth/login/single_tenant")
        |> assert_path(~p"/dashboard")
        |> visit(~p"/query")
        |> wait_for_monaco_editor()
        |> replace_monaco_text(query)
        |> click("button", "Run query")

      conn
      |> assert_has("table", text: "answer")
      |> assert_has("table", text: "42")
      |> assert_has("table", text: "monaco e2e result")
    end
  end
end
