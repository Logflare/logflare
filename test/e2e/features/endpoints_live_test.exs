defmodule E2e.Features.EndpointsLiveTest do
  use Logflare.FeatureCase, async: false

  import LogflareWeb.MonacoEditorTestUtils

  describe "endpoints page Monaco editor" do
    TestUtils.setup_single_tenant(seed_user: true, backend_type: :postgres)

    test "creates an endpoint with the query entered in Monaco", %{conn: conn} do
      endpoint_name = "MonacoBaseline#{System.unique_integer([:positive])}"
      endpoint_query = "select 42 as answer, 'endpoint monaco e2e result' as label"

      conn
      |> visit(~p"/auth/login/single_tenant")
      |> assert_path(~p"/dashboard")
      |> visit(~p"/endpoints/new")
      |> wait_for_monaco_editor()
      |> fill_input("input[name='endpoint[name]']", endpoint_name)
      |> replace_monaco_text(endpoint_query)
      |> wait_for_input_value("input[name='endpoint[query]']", endpoint_query)
      |> click("button", "Save changes")
      |> assert_has("h1,h2,h3,h4,h5", text: endpoint_name)
      |> assert_has("code", text: endpoint_query)
    end
  end
end
