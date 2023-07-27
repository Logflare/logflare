defmodule LogflareWeb.EndpointsControllerTest do
  use LogflareWeb.ConnCase
  alias Logflare.SingleTenant

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
      |> stub(:bigquery_jobs_query, fn conn, _proj_id, _opts ->
        assert {Tesla.Adapter.Finch, :call, [[name: Logflare.FinchQuery, receive_timeout: _]]} =
                 conn.adapter

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

    test "GET query with user.api_key", %{conn: init_conn, user: user} do
      endpoint = insert(:endpoint, user: user, enable_auth: true)

      conn =
        init_conn
        |> put_req_header("x-api-key", user.api_key)
        |> get("/endpoints/query/#{endpoint.token}")

      assert [%{"event_message" => "some event message"}] = json_response(conn, 200)["result"]
      assert conn.halted == false

      # should be able to query with endpoint name (deprecated path)
      conn =
        init_conn
        |> put_req_header("x-api-key", user.api_key)
        |> get("/api/endpoints/query/name/#{endpoint.name}")

      assert [%{"event_message" => "some event message"}] = json_response(conn, 200)["result"]
      assert conn.halted == false

      # should be able to query with endpoint name
      conn =
        init_conn
        |> put_req_header("x-api-key", user.api_key)
        |> get("/api/endpoints/query/#{endpoint.name}")

      assert [%{"event_message" => "some event message"}] = json_response(conn, 200)["result"]
      assert conn.halted == false
    end

    test "GET query with other user's api key", %{conn: init_conn, user: user} do
      user2 = insert(:user)
      endpoint = insert(:endpoint, user: user, enable_auth: true)

      conn =
        init_conn
        |> put_req_header("x-api-key", user2.api_key)
        |> get("/endpoints/query/#{endpoint.token}")

      assert conn.status == 401
      assert conn.halted == true
    end

    # ticket: https://www.notion.so/supabase/bug-Logflare-endpoint-query-by-name-sometimes-mistakes-a-string-for-a-uuid-0034097613954fafab27ed608e287f70?pvs=4
    test "bug: query by name uuid pattern check", %{conn: init_conn, user: user} do
      for name <- [
            "logs.all.staging",
            "logs-all-staging"
          ] do
        endpoint = insert(:endpoint, name: name, user: user, enable_auth: true)

        conn =
          init_conn
          |> put_req_header("x-api-key", user.api_key)
          |> get("/api/endpoints/query/#{endpoint.name}")

        assert [%{"event_message" => "some event message"}] = json_response(conn, 200)["result"]
        assert conn.halted == false
      end
    end
  end

  describe "single tenant endpoint query" do
    setup :set_mimic_global

    TestUtils.setup_single_tenant(seed_user: true)

    setup do
      user = SingleTenant.get_default_user()
      # mock goth behaviour
      Goth
      |> stub(:fetch, fn _mod -> {:ok, %Goth.Token{token: "auth-token"}} end)

      {:ok, user: user}
    end

    test "single tenant endpoint GET", %{conn: conn, user: user} do
      endpoint = insert(:endpoint, user: user, enable_auth: true)

      GoogleApi.BigQuery.V2.Api.Jobs
      |> stub(:bigquery_jobs_query, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response()}
      end)

      conn =
        conn
        |> put_req_header("x-api-key", user.api_key)
        |> get("/endpoints/query/#{endpoint.token}")

      assert [%{"event_message" => "some event message"}] = json_response(conn, 200)["result"]
      assert conn.halted == false
    end
  end

  describe "single tenant ui" do
    TestUtils.setup_single_tenant(seed_user: true)

    test "can view endpoints page", %{conn: conn} do
      conn = get(conn, "/endpoints")
      assert html_response(conn, 200) =~ "/endpoints"
    end

    test "can make new endpoint", %{conn: conn} do
      conn = get(conn, "/endpoints/new")
      assert html_response(conn, 200) =~ "/endpoints"
    end
  end


  describe "single tenant supabase mode" do
    TestUtils.setup_single_tenant(seed_user: true, supabase_mode: true, backend_type: :postgres)

    setup do
      SingleTenant.create_supabase_sources()
      SingleTenant.create_supabase_endpoints
      %{user: SingleTenant.get_default_user()}
    end
    test "GET a basic query", %{conn: conn, user: user} do
      # query the logs.all endpoint

      conn =
        conn
        |> put_req_header("x-api-key", user.api_key)
        |> get(~p"/endpoints/query/logs.all?#{%{
          sql: ~s(select "hello" as world from edge_logs)
        }}")

      assert [%{"world" => "hello"}] = json_response(conn, 200)["result"]
      assert conn.halted == false
    end
  end

end
