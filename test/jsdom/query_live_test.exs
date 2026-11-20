defmodule LogflareWeb.Jsdom.QueryLiveTest do
  use LogflareWeb.ConnCase

  @moduletag :jsdom

  setup %{conn: conn} do
    insert(:plan)
    user = insert(:user)
    conn = login_user(conn, user)
    {:ok, user: user, conn: conn}
  end

  describe "query page" do
    test "run a valid query", %{conn: conn} do
      start_supervised!(PhoenixTestJsdom)

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, opts ->
        assert [body: %_{useQueryCache: false}] = opts

        {:ok, TestUtils.gen_bq_response([%{"ts" => "some-data"}])}
      end)

      query = "select current_timestamp() as ts"

      {:ok, view, _html} = live_with_redirect(conn, ~p"/query") |> PhoenixTestJsdom.mount()

      view
      |> PhoenixTestJsdom.wait_for(".monaco-editor textarea.inputarea", 10_000)

      assert {:ok, ^query} =
               PhoenixTestJsdom.exec_js(view, """
               const query = #{Jason.encode!(query)};
               const model = window.monaco.editor.getModels()[0];
               model.setValue(query);
               model.getValue();
               """)

      view
      |> PhoenixTestJsdom.click("Run query", selector: "button")
      |> PhoenixTestJsdom.wait_for("table", 10_000)

      html = PhoenixTestJsdom.render(view)

      assert html =~ "Ran query successfully"
      assert html =~ ~r/1 .+ processed/

      assert PhoenixTestJsdom.current_path(view) =~ ~r/current_timestamp/
      assert html =~ "some-data"
    end
  end
end
