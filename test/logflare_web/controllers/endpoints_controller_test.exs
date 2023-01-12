defmodule LogflareWeb.EndpointsControllerTest do
  use LogflareWeb.ConnCase

  describe "query" do
    setup :set_mimic_global

    setup do
      source = build(:source, rules: [])
      user = insert(:user, sources: [source])
      _plan = insert(:plan, name: "Free")

      # mock goth behaviour
      Goth
      |> stub(:fetch, fn _mod -> {:ok, %Goth.Token{token: "auth-token"}} end)

      GoogleApi.BigQuery.V2.Api.Jobs
      |> stub(:bigquery_jobs_query, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response()}
      end)

      {:ok, user: user, source: source}
    end

    test "GET query", %{conn: init_conn, user: user} do
      endpoint = insert(:endpoint, user: user, enable_auth: false)

      conn =
        init_conn
        |> get("/endpoints/query/#{endpoint.token}")

      assert [%{"event_message" => "some event message"}] = json_response(conn, 200)["result"]
      assert conn.halted == false

      conn =
        init_conn
        |> get("/api/endpoints/query/#{endpoint.token}")

      assert [%{"event_message" => "some event message"}] = json_response(conn, 200)["result"]
      assert conn.halted == false
    end
  end

  describe "ui" do
    @valid_params %{
      name: "current date",
      query: "/*some comment*/\nselect current_date() as date"
    }
    setup %{conn: conn} do
      plan = insert(:plan, name: "Free", type: "standard")
      user = insert(:user)
      _source = insert(:source, user: user)
      _billing_account = insert(:billing_account, user: user, stripe_plan_id: plan.stripe_id)
      user = user |> Logflare.Repo.preload(:billing_account)
      conn = conn |> put_session(:user_id, user.id) |> assign(:user, user)
      [conn: conn]
    end

    test "Endpoints index", %{conn: conn} do
      conn = get(conn, Routes.endpoints_path(conn, :index))
      assert html_response(conn, 200) =~ "/endpoints"
    end

    test "Edit Endpoint", %{conn: conn} do
      conn =
        conn
        |> post(Routes.endpoints_path(conn, :create), query: @valid_params)

      assert %{id: id} = redirected_params(conn)

      conn =
        conn
        |> get(Routes.endpoints_path(conn, :edit, id))

      html = html_response(conn, 200)
      assert html =~ "Query Sandboxing"
      assert html =~ @valid_params.query
    end

    test "Endpoint for User", %{conn: conn} do
      conn =
        conn
        |> post(Routes.endpoints_path(conn, :create), query: @valid_params)

      assert %{id: id} = redirected_params(conn)
      assert redirected_to(conn) == Routes.endpoints_path(conn, :show, id)

      conn = conn |> get(Routes.endpoints_path(conn, :show, id))
      assert html = html_response(conn, 200)
      assert html =~ "/endpoints/"
      assert html =~ "current date"
    end
  end
end
