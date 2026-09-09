defmodule E2e.Features.AlertsLiveTest do
  use Logflare.FeatureCase, async: false

  import LogflareWeb.MonacoEditorTestUtils

  describe "alerts page Monaco editor" do
    TestUtils.setup_single_tenant(seed_user: true, backend_type: :postgres)

    test "creates an alert with the query entered in Monaco", %{conn: conn} do
      alert_name = "MonacoAlert#{System.unique_integer([:positive])}"
      alert_query = "select 42 as answer, 'alert monaco e2e result' as label"
      cron = "*/5 * * * *"

      conn
      |> visit(~p"/auth/login/single_tenant")
      |> assert_path(~p"/dashboard")
      |> visit(~p"/alerts/new")
      |> wait_for_monaco_editor()
      |> fill_input("input[name='alert[name]']", alert_name)
      |> replace_monaco_text(alert_query)
      |> wait_for_input_value("input[name='alert[query]']", alert_query)
      |> fill_input("input[name='alert[cron]']", cron)
      |> click("button", "Save changes")
      |> assert_has("h2", text: alert_name)
      |> assert_has("code", text: alert_query)
      |> assert_has("li", text: cron)
    end
  end
end
