defmodule LogflareWeb.Api.QueryControllerTest do
  use LogflareWeb.ConnCase

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.DataCase

  setup do
    insert(:plan)
    user = insert(:user)

    {:ok, user: user}
  end

  test "no query param provided returns a JSON 400 error", %{conn: conn, user: user} do
    conn =
      conn
      |> add_access_token(user, ~w(private))
      |> get(~p"/api/query")

    assert ["application/json; charset=utf-8"] = get_resp_header(conn, "content-type")

    assert %{"error" => message} =
             conn
             |> json_response(400)

    assert message =~ "No query params provided"
  end

  describe "validate/2" do
    test "valid sql query returns 200 ok", %{conn: conn, user: user} do
      conn =
        conn
        |> add_access_token(user, ~w(private))
        |> get(~p"/api/query/parse?#{[sql: ~s|select current_datetime() as 'my_time'|]}")

      assert %{"result" => %{"parameters" => []}} = json_response(conn, 200)
    end

    test "valid deprecated ch_sql query param returns 200 ok", %{conn: conn, user: user} do
      conn =
        conn
        |> add_access_token(user, ~w(private))
        |> get(~p"/api/query/parse?#{[ch_sql: ~s|select now() as 'my_time'|]}")

      assert %{"result" => %{"parameters" => []}} = json_response(conn, 200)
    end

    test "parse accepts backend_id to select the backend language for sql", %{
      conn: conn,
      user: user
    } do
      backend = insert(:backend, user: user, type: :clickhouse)

      conn =
        conn
        |> add_access_token(user, ~w(private))
        |> get(
          ~p"/api/query/parse?#{[sql: ~s|select now() as 'my_time'|, backend_id: backend.id]}"
        )

      assert %{"result" => %{"parameters" => []}} = json_response(conn, 200)
    end

    test "invalid sql query returns a JSON 400 error", %{conn: conn, user: user} do
      conn =
        conn
        |> add_access_token(user, ~w(private))
        |> get(~p"/api/query/parse?#{[bq_sql: ~s|update something SET test = 'something'|]}")

      assert ["application/json; charset=utf-8"] = get_resp_header(conn, "content-type")

      assert %{"error" => err} =
               conn
               |> json_response(400)

      assert err =~ "SELECT"
    end
  end

  describe "query with bq" do
    test "?sql= query param", %{
      conn: conn,
      user: user
    } do
      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 2, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"my_time" => "123"}])}
      end)

      conn =
        conn
        |> add_access_token(user, ~w(private))
        |> get(~p"/api/query?#{[bq_sql: ~s|select current_datetime() as 'my_time'|]}")

      assert ["application/json; charset=utf-8"] = get_resp_header(conn, "content-type")

      response = json_response(conn, 200)

      assert %{"result" => [%{"my_time" => "123"}]} = response

      response =
        conn
        |> recycle()
        |> add_access_token(user, ~w(private))
        |> get(~p"/api/query?#{[sql: ~s|select current_datetime() as 'my_time'|]}")
        |> json_response(200)

      assert %{"result" => [%{"my_time" => "123"}]} = response
    end

    test "BQ errors return a generic response", %{
      conn: conn,
      user: user
    } do
      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:error, TestUtils.gen_bq_error("some error")}
      end)

      response =
        conn
        |> add_access_token(user, ~w(private))
        |> get(~p"/api/query?#{[bq_sql: ~s|select current_datetime() as 'my_time'|]}")
        |> json_response(400)

      assert %{
               "error" =>
                 "Backend error! Retry your query. Please contact support if this continues."
             } = response

      refute inspect(response) =~ "some error"
    end

    test "deprecated param bq_sql has precedence over others", %{
      conn: conn,
      user: user
    } do
      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, opts ->
        assert opts[:body].query =~ "preferred_value"
        refute opts[:body].query =~ "legacy_value"
        {:ok, TestUtils.gen_bq_response([%{"preferred_value" => "123"}])}
      end)

      response =
        conn
        |> add_access_token(user, ~w(private))
        |> get(
          ~p"/api/query?#{[bq_sql: ~s|select 1 as preferred_value|, ch_sql: ~s|select 2 as legacy_value|]}"
        )
        |> json_response(200)

      assert %{"result" => [%{"preferred_value" => "123"}]} = response
    end
  end

  describe "query with pg_sql" do
    setup do
      cfg = Application.get_env(:logflare, Logflare.Repo)

      url = "postgresql://#{cfg[:username]}:#{cfg[:password]}@#{cfg[:hostname]}/#{cfg[:database]}"

      user = insert(:user)
      source = insert(:source, user: user, name: "c")

      backend =
        insert(:backend,
          type: :postgres,
          config: %{url: url},
          sources: [source],
          user: user
        )

      PostgresAdaptor.create_repo(backend)
      PostgresAdaptor.create_events_table({source, backend})

      on_exit(fn ->
        PostgresAdaptor.destroy_instance({source, backend})
      end)

      %{source: source, user: user}
    end

    test "?pg_sql= query param uses the first PostgreSQL backend", %{
      conn: conn,
      user: user
    } do
      query = ~S|select now() as "my_time"|

      response =
        conn
        |> add_access_token(user, ~w(private))
        |> get(~p"/api/query?#{[pg_sql: query]}")
        |> json_response(200)

      assert %{"result" => [%{"my_time" => _}]} = response
    end

    test "?sql= with backend_id infers pg_sql language", %{
      conn: conn,
      user: user
    } do
      backend = Logflare.Backends.list_backends_by_user_id(user.id) |> hd()
      query = ~S|select now() as "my_time"|

      response =
        conn
        |> add_access_token(user, ~w(private))
        |> get(~p"/api/query?#{[sql: query, backend_id: backend.id]}")
        |> json_response(200)

      assert %{"result" => [%{"my_time" => _}]} = response
    end
  end

  describe "backend_id parameter" do
    test "?sql= with backend_id uses backend's language", %{conn: conn, user: user} do
      {_source, backend} = DataCase.setup_clickhouse_test(user: user)
      start_supervised!({ClickHouseAdaptor, backend})

      query = "SELECT dummy AS my_time FROM system.one LIMIT 1 BY dummy"

      assert conn
             |> add_access_token(user, ~w(private))
             |> get(~p"/api/query?#{[sql: query]}")
             |> json_response(400),
             "fails when parsed for bq_sql"

      response =
        conn
        |> add_access_token(user, ~w(private))
        |> get(~p"/api/query?#{[sql: query, backend_id: backend.id]}")
        |> json_response(200)

      assert %{"result" => [%{"my_time" => 0}]} = response
    end

    test "?ch_sql= with backend_id", %{conn: conn, user: user} do
      {_source, backend} = DataCase.setup_clickhouse_test(user: user)
      start_supervised!({ClickHouseAdaptor, backend})

      query = "SELECT dummy AS my_time FROM system.one LIMIT 1 BY dummy"

      response =
        conn
        |> add_access_token(user, ~w(private))
        |> get(~p"/api/query?#{[ch_sql: query, backend_id: backend.id]}")
        |> json_response(200)

      assert %{"result" => [%{"my_time" => 0}]} = response
    end

    test "?ch_sql= without backend_id uses ClickHouse backend", %{
      conn: conn,
      user: user
    } do
      {_source, backend} = DataCase.setup_clickhouse_test(user: user)
      start_supervised!({ClickHouseAdaptor, backend})

      query = "SELECT dummy AS my_time FROM system.one LIMIT 1 BY dummy"

      response =
        conn
        |> add_access_token(user, ~w(private))
        |> get(~p"/api/query?#{[ch_sql: query]}")
        |> json_response(200)

      assert %{"result" => [%{"my_time" => 0}]} = response
    end

    test "bq_sql param with backend_id executes query", %{conn: conn, user: user} do
      {_source, clickhouse_backend} = DataCase.setup_clickhouse_test(user: user)
      start_supervised!({ClickHouseAdaptor, clickhouse_backend})

      query = ~S|select 1 as my_time|

      response =
        conn
        |> add_access_token(user, ~w(private))
        |> get(~p"/api/query?#{[bq_sql: query, backend_id: clickhouse_backend.id]}")
        |> json_response(200)

      assert %{"result" => [%{"my_time" => 1}]} = response
    end

    test "?sql= takes precedence over deprecated params", %{conn: conn, user: user} do
      {_source, backend} = DataCase.setup_clickhouse_test(user: user)
      start_supervised!({ClickHouseAdaptor, backend})

      response =
        conn
        |> add_access_token(user, ~w(private))
        |> get(
          ~p"/api/query?#{[sql: ~s|SELECT dummy AS my_time FROM system.one LIMIT 1 BY dummy|, bq_sql: ~s|select 2 as legacy_value|, backend_id: backend.id]}"
        )
        |> json_response(200)

      assert %{"result" => [%{"my_time" => 0}]} = response
    end

    test "invalid backend_id returns error", %{conn: conn, user: user} do
      query = ~S|select now() as "my_time"|

      response =
        conn
        |> add_access_token(user, ~w(private))
        |> get(~p"/api/query?#{[sql: query, backend_id: "invalid"]}")
        |> json_response(400)

      assert %{"error" => msg} = response
      assert msg =~ "Invalid backend_id"
    end

    test "non-existent backend_id returns error", %{conn: conn, user: user} do
      query = ~S|select now() as "my_time"|

      response =
        conn
        |> add_access_token(user, ~w(private))
        |> get(~p"/api/query?#{[sql: query, backend_id: 999_999]}")
        |> json_response(400)

      assert %{"error" => "Backend not found"} = response
    end

    test "backend belonging to another user returns error", %{conn: conn, user: user} do
      other_user = insert(:user)
      backend = insert(:backend, user: other_user, type: :clickhouse)

      query = ~S|select now() as "my_time"|

      response =
        conn
        |> add_access_token(user, ~w(private))
        |> get(~p"/api/query?#{[sql: query, backend_id: backend.id]}")
        |> json_response(400)

      assert %{"error" => "Backend not found"} = response
    end

    test "backend that cannot be queried returns error", %{conn: conn, user: user} do
      backend = insert(:backend, user: user, type: :webhook)

      query = ~S|select now() as "my_time"|

      response =
        conn
        |> add_access_token(user, ~w(private))
        |> get(~p"/api/query?#{[sql: query, backend_id: backend.id]}")
        |> json_response(400)

      assert %{"error" => "Backend does not support querying"} = response
    end
  end
end
