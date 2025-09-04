defmodule LogflareWeb.EndpointsControllerTest do
  use LogflareWeb.ConnCase, async: false

  alias Logflare.SingleTenant
  alias Logflare.Backends
  alias Logflare.Sources.Source
  alias Logflare.Sources
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.Google.BigQuery.GenUtils

  setup do
    start_supervised!(AllLogsLogged)
    :ok
  end

  describe "query" do
    setup do
      source = build(:source, rules: [])
      user = insert(:user, sources: [source])
      _plan = insert(:plan, name: "Free")

      {:ok, user: user, source: source}
    end

    test "GET query", %{conn: init_conn, user: user} do
      endpoint = insert(:endpoint, user: user, enable_auth: false)
      pid = self()

      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, _opts ->
        {:error, :failed_request}
      end)

      conn =
        init_conn
        |> get(~p"/endpoints/query/#{endpoint.token}")

      response =
        conn
        |> json_response(200)
        |> assert_schema("EndpointQuery")

      assert response.error == %{"message" => "failed_request"}
      refute response.result
      refute conn.halted

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, fn conn, _proj_id, _opts ->
        send(pid, {:conn_adapter, conn.adapter})

        {:ok, TestUtils.gen_bq_response()}
      end)

      conn =
        init_conn
        |> get(~p"/endpoints/query/#{endpoint.token}")

      TestUtils.retry_assert(fn ->
        assert_received {:conn_adapter,
                         {Tesla.Adapter.Finch, :call,
                          [[name: Logflare.FinchQuery, receive_timeout: _]]}}
      end)

      response =
        conn
        |> json_response(200)
        |> assert_schema("EndpointQuery")

      assert [
               %{
                 "event_message" => "some event message",
                 "id" => _id,
                 "timestamp" => _timestamp
               }
             ] = response.result

      refute response.error
      refute conn.halted

      reject(&GoogleApi.BigQuery.V2.Api.Jobs.bigquery_jobs_query/3)

      conn =
        init_conn
        |> get(~p"/endpoints/query/#{endpoint.token}")

      response =
        conn
        |> json_response(200)
        |> assert_schema("EndpointQuery")

      assert [
               %{
                 "event_message" => "some event message",
                 "id" => _id,
                 "timestamp" => _timestamp
               }
             ] = response.result

      refute response.error

      refute conn.halted
    end

    test "GET query with user.api_key", %{conn: init_conn, user: user} do
      endpoint = insert(:endpoint, user: user, enable_auth: true)

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response()}
      end)

      conn =
        init_conn
        |> put_req_header("x-api-key", user.api_key)
        |> get(~p"/endpoints/query/#{endpoint.token}")

      response =
        conn
        |> json_response(200)
        |> assert_schema("EndpointQuery")

      assert [
               %{
                 "event_message" => "some event message",
                 "id" => _id,
                 "timestamp" => _timestamp
               }
             ] = response.result

      refute response.error
      refute conn.halted

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response()}
      end)

      # should be able to query with endpoint name (deprecated path)
      conn =
        init_conn
        |> put_req_header("x-api-key", user.api_key)
        |> get(~p"/api/endpoints/query/name/#{endpoint.name}")

      response =
        conn
        |> json_response(200)
        |> assert_schema("EndpointQuery")

      assert [
               %{
                 "event_message" => "some event message",
                 "id" => _id,
                 "timestamp" => _timestamp
               }
             ] = response.result

      refute response.error
      refute conn.halted

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response()}
      end)

      # should be able to query with endpoint name
      conn =
        init_conn
        |> put_req_header("x-api-key", user.api_key)
        |> get(~p"/api/endpoints/query/#{endpoint.name}")

      response =
        conn
        |> json_response(200)
        |> assert_schema("EndpointQuery")

      assert [
               %{
                 "event_message" => "some event message",
                 "id" => _id,
                 "timestamp" => _timestamp
               }
             ] = response.result

      refute response.error
      refute conn.halted
    end

    test "GET query with other user's api key", %{conn: init_conn, user: user} do
      user2 = insert(:user)
      endpoint = insert(:endpoint, user: user, enable_auth: true)

      conn =
        init_conn
        |> put_req_header("x-api-key", user2.api_key)
        |> get(~p"/endpoints/query/#{endpoint.token}")

      assert conn
             |> json_response(401)
             |> assert_schema("Unauthorized") == %{"error" => "Unauthorized"}

      assert conn.halted == true
    end

    # ticket: https://www.notion.so/supabase/bug-Logflare-endpoint-query-by-name-sometimes-mistakes-a-string-for-a-uuid-0034097613954fafab27ed608e287f70?pvs=4
    test "bug: query by name uuid pattern check", %{conn: init_conn, user: user} do
      for name <- [
            "logs.all.staging",
            "logs-all-staging"
          ] do
        GoogleApi.BigQuery.V2.Api.Jobs
        |> expect(:bigquery_jobs_query, fn _conn, _proj_id, _opts ->
          {:ok, TestUtils.gen_bq_response()}
        end)

        endpoint = insert(:endpoint, name: name, user: user, enable_auth: true)

        conn =
          init_conn
          |> put_req_header("x-api-key", user.api_key)
          |> get(~p"/api/endpoints/query/#{endpoint.name}")

        response =
          conn
          |> json_response(200)
          |> assert_schema("EndpointQuery")

        assert [
                 %{
                   "event_message" => "some event message",
                   "id" => _id,
                   "timestamp" => _timestamp
                 }
               ] = response.result

        refute response.error
        refute conn.halted
      end
    end
  end

  describe "bigquery with labels" do
    setup do
      _plan = insert(:plan, name: "Free")
      user = insert(:user)
      source = build(:source, user: user)
      {:ok, user: user, source: source}
    end

    test "reference params in label, my_label=@my_param", %{
      conn: conn,
      user: user
    } do
      pid = self()

      GoogleApi.BigQuery.V2.Api.Jobs
      |> stub(:bigquery_jobs_query, fn _conn, _proj_id, opts ->
        send(pid, opts[:body].labels)
        {:ok, TestUtils.gen_bq_response()}
      end)

      endpoint =
        insert(:endpoint,
          user: user,
          enable_auth: true,
          query: "with a as (select 1 as b) select b from a",
          labels: ",my_label=@my_param,other_value,my=value,"
        )

      conn =
        conn
        |> put_req_header("x-api-key", user.api_key)
        |> put_req_header("content-type", "application/json")
        |> put_req_header("lf-endpoint-labels", "other_value=1234,omit=334")
        |> get(
          ~p"/api/endpoints/query/#{endpoint.name}?#{%{"other_value" => 1234, "omit" => 333, "my_param" => "my_value", "sql" => "select 2"}}"
        )

      assert [_] = json_response(conn, 200)["result"]
      assert conn.halted == false
      assert_received labels

      assert labels == %{
               "my_label" => "my_value",
               "other_value" => "1234",
               "endpoint_id" => GenUtils.format_value(endpoint.id),
               "logflare_account" => GenUtils.format_value(user.id),
               "logflare_plan" => "free",
               "managed_by" => "logflare",
               "my" => "value"
             }
    end
  end

  describe "sandboxed query" do
    setup do
      _plan = insert(:plan, name: "Free")
      user = insert(:user)
      source = build(:source, user: user)
      {:ok, user: user, source: source}
    end

    test "params in GET body", %{conn: conn, user: user} do
      pid = self()

      GoogleApi.BigQuery.V2.Api.Jobs
      |> stub(:bigquery_jobs_query, fn _conn, _proj_id, opts ->
        send(pid, {:sql, opts[:body].query})

        {:ok, TestUtils.gen_bq_response()}
      end)

      endpoint =
        insert(:endpoint,
          user: user,
          enable_auth: true,
          sandboxable: true,
          query: "with a as (select 1 as b) select b from a"
        )

      conn =
        conn
        |> put_req_header("x-api-key", user.api_key)
        |> put_req_header("content-type", "application/json")
        |> get(~p"/api/endpoints/query/#{endpoint.name}", Jason.encode!(%{sql: "select 2"}))

      assert [%{"event_message" => "some event message"}] = json_response(conn, 200)["result"]
      assert conn.halted == false
      assert_received {:sql, sql}
      assert String.downcase(sql) =~ "select 2"
    end

    test "params in POST body", %{conn: conn, user: user} do
      pid = self()

      GoogleApi.BigQuery.V2.Api.Jobs
      |> stub(:bigquery_jobs_query, fn _conn, _proj_id, opts ->
        send(pid, {:sql, opts[:body].query})

        {:ok, TestUtils.gen_bq_response()}
      end)

      endpoint =
        insert(:endpoint,
          user: user,
          enable_auth: true,
          sandboxable: true,
          query: "with a as (select 1 as b) select b from a"
        )

      conn =
        conn
        |> put_req_header("x-api-key", user.api_key)
        |> post(~p"/api/endpoints/query/#{endpoint.name}", %{sql: "select 2"})

      assert [%{"event_message" => "some event message"}] = json_response(conn, 200)["result"]
      assert conn.halted == false
      assert_received {:sql, sql}
      assert String.downcase(sql) =~ "select 2"
    end
  end

  describe "single tenant endpoint query" do
    setup :set_mimic_global

    TestUtils.setup_single_tenant(seed_user: true)

    setup do
      user = SingleTenant.get_default_user()
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
        |> get(~p"/endpoints/query/#{endpoint.token}")

      assert [%{"event_message" => "some event message"}] = json_response(conn, 200)["result"]
      assert conn.halted == false
    end
  end

  describe "single tenant ui" do
    TestUtils.setup_single_tenant(seed_user: true)

    test "can view endpoints page", %{conn: conn} do
      conn = get(conn, ~p"/endpoints")
      assert html_response(conn, 200) =~ ~p"/endpoints"
    end

    test "can make new endpoint", %{conn: conn} do
      conn = get(conn, ~p"/endpoints/new")
      assert html_response(conn, 200) =~ ~p"/endpoints"
    end
  end

  describe "single tenant supabase mode" do
    TestUtils.setup_single_tenant(
      seed_user: true,
      supabase_mode: true,
      backend_type: :postgres,
      pg_schema: "my_schema",
      seed_backend: true
    )

    setup do
      SingleTenant.create_supabase_sources()
      SingleTenant.create_supabase_endpoints()
      SingleTenant.ensure_supabase_sources_started()
      %{user: SingleTenant.get_default_user()}
    end

    test "GET a basic sandboxed query", %{conn: conn, user: user} do
      conn =
        conn
        |> put_req_header("x-api-key", user.api_key)
        |> get(~p"/endpoints/query/logs.all?#{%{sql: ~s(select 'hello' as world)}}")

      assert [%{"world" => "hello"}] = json_response(conn, 200)["result"]
      assert conn.halted == false
    end

    test "GET a basic sandboxed query with from table", %{conn: initial_conn, user: user} do
      for source <- Logflare.Repo.all(Source) do
        source = Sources.preload_defaults(source)

        Backends.ingest_logs(
          [%{"event_message" => "some message", "project" => "default"}],
          source
        )
      end

      :timer.sleep(2_000)

      params = %{
        iso_timestamp_start:
          DateTime.utc_now() |> DateTime.add(-3, :day) |> DateTime.to_iso8601(),
        project: "default",
        project_tier: "ENTERPRISE",
        sql: "select  timestamp,  event_message, metadata from edge_logs"
      }

      conn =
        initial_conn
        |> put_req_header("x-api-key", user.api_key)
        |> get(~p"/endpoints/query/logs.all?#{params}")

      assert [%{"event_message" => "some message", "timestamp" => timestamp}] =
               json_response(conn, 200)["result"]

      # render as unix microsecond
      assert inspect(timestamp) |> String.length() == 16
      assert conn.halted == false

      # test a logs ui query
      params = %{
        iso_timestamp_start:
          DateTime.utc_now() |> DateTime.add(-3, :day) |> DateTime.to_iso8601(),
        project: "default",
        project_tier: "ENTERPRISE",
        sql:
          "select id, timestamp, event_message, request.method, request.path, response.status_code from edge_logs cross join unnest(metadata) as m cross join unnest(m.request) as request cross join unnest(m.response) as response limit 100 "
      }

      conn =
        initial_conn
        |> put_req_header("x-api-key", user.api_key)
        |> get(~p"/endpoints/query/logs.all?#{params}")

      assert [%{"event_message" => "some message", "id" => log_id}] =
               json_response(conn, 200)["result"]

      assert conn.halted == false

      # different project filter
      params = %{
        iso_timestamp_start:
          DateTime.utc_now() |> DateTime.add(-3, :day) |> DateTime.to_iso8601(),
        project: "other",
        project_tier: "ENTERPRISE",
        sql:
          "select id, timestamp, event_message, request.method, request.path, response.status_code from edge_logs cross join unnest(metadata) as m cross join unnest(m.request) as request cross join unnest(m.response) as response limit 100 "
      }

      conn =
        initial_conn
        |> put_req_header("x-api-key", user.api_key)
        |> get(~p"/endpoints/query/logs.all?#{params}")

      assert [] = json_response(conn, 200)["result"]
      assert conn.halted == false

      # log chart sql
      params = %{
        iso_timestamp_start:
          DateTime.utc_now() |> DateTime.add(-3, :day) |> DateTime.to_iso8601(),
        project: "default",
        project_tier: "ENTERPRISE",
        sql:
          "\nSELECT\n-- event-chart\n  timestamp_trunc(t.timestamp, minute) as timestamp,\n  count(t.timestamp) as count\nFROM\n  edge_logs t\n  cross join unnest(t.metadata) as metadata \n  cross join unnest(metadata.request) as request \n  cross join unnest(metadata.response) as response\n  where t.timestamp > '2023-08-05T09:00:00.000Z'\nGROUP BY\ntimestamp\nORDER BY\n  timestamp ASC\n"
      }

      conn =
        initial_conn
        |> put_req_header("x-api-key", user.api_key)
        |> get(~p"/endpoints/query/logs.all?#{params}")

      assert [%{"count" => 1}] = json_response(conn, 200)["result"]
      assert conn.halted == false

      # log chart sql
      params = %{
        iso_timestamp_start:
          DateTime.utc_now() |> DateTime.add(-3, :day) |> DateTime.to_iso8601(),
        project: "default",
        project_tier: "ENTERPRISE",
        sql:
          "select id, timestamp, event_message, metadata from edge_logs where id = '#{log_id}' limit 1"
      }

      conn =
        initial_conn
        |> put_req_header("x-api-key", user.api_key)
        |> get(~p"/endpoints/query/logs.all?#{params}")

      assert [%{"event_message" => "some message", "id" => ^log_id}] =
               json_response(conn, 200)["result"]

      assert conn.halted == false
    end
  end

  describe "PII redaction" do
    setup do
      _plan = insert(:plan, name: "Free")
      user = insert(:user)
      {:ok, user: user}
    end

    test "redacts IP addresses when redact_pii is enabled", %{conn: init_conn, user: user} do
      endpoint = insert(:endpoint, user: user, enable_auth: false, redact_pii: true)

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, fn _conn, _proj_id, _opts ->
        bq_response =
          TestUtils.gen_bq_response([
            %{"ip_address" => "192.168.1.1", "event_message" => "User 10.0.0.1 connected"}
          ])

        {:ok, bq_response}
      end)

      conn =
        init_conn
        |> get(~p"/endpoints/query/#{endpoint.token}")

      response =
        conn
        |> json_response(200)
        |> assert_schema("EndpointQuery")

      assert [
               %{
                 "ip_address" => "REDACTED",
                 "event_message" => "User REDACTED connected"
               }
             ] = response.result

      refute response.error
      refute conn.halted
    end

    test "does not redact IP addresses when redact_pii is disabled", %{
      conn: init_conn,
      user: user
    } do
      endpoint = insert(:endpoint, user: user, enable_auth: false, redact_pii: false)

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, fn _conn, _proj_id, _opts ->
        bq_response =
          TestUtils.gen_bq_response([
            %{"ip_address" => "192.168.1.1", "message" => "User 10.0.0.1 connected"}
          ])

        {:ok, bq_response}
      end)

      conn =
        init_conn
        |> get(~p"/endpoints/query/#{endpoint.token}")

      response =
        conn
        |> json_response(200)
        |> assert_schema("EndpointQuery")

      assert [
               %{
                 "ip_address" => "192.168.1.1",
                 "message" => "User 10.0.0.1 connected"
               }
             ] = response.result

      refute response.error
      refute conn.halted
    end
  end
end
