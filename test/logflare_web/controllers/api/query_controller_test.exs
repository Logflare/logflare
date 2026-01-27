defmodule LogflareWeb.Api.QueryControllerTest do
  use LogflareWeb.ConnCase

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor

  setup do
    insert(:plan)
    user = insert(:user)

    {:ok, user: user}
  end

  test "no query param provided", %{conn: conn, user: user} do
    conn
    |> add_access_token(user, ~w(private))
    |> get(~p"/api/query")
    |> json_response(400)
  end

  describe "validate/2" do
    test "valid sql query returns 200 ok", %{conn: conn, user: user} do
      conn =
        conn
        |> add_access_token(user, ~w(private))
        |> get(~p"/api/query/parse?#{[sql: ~s|select current_datetime() as 'my_time'|]}")

      assert %{"result" => %{"parameters" => []}} = json_response(conn, 200)
    end

    test "invalid valid sql query returns 200 ok", %{conn: conn, user: user} do
      conn =
        conn
        |> add_access_token(user, ~w(private))
        |> get(~p"/api/query/parse?#{[bq_sql: ~s|update something SET test = 'something'|]}")

      assert %{"error" => err} = json_response(conn, 400)
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

      response =
        conn
        |> add_access_token(user, ~w(private))
        |> get(~p"/api/query?#{[bq_sql: ~s|select current_datetime() as 'my_time'|]}")
        |> json_response(200)

      assert %{"result" => [%{"my_time" => "123"}]} = response

      response =
        conn
        |> recycle()
        |> add_access_token(user, ~w(private))
        |> get(~p"/api/query?#{[sql: ~s|select current_datetime() as 'my_time'|]}")
        |> json_response(200)

      assert %{"result" => [%{"my_time" => "123"}]} = response
    end

    test "BQ errors are propagated", %{
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

      assert %{"error" => %{"message" => "some error"}} = response
    end
  end

  describe "pg_sql" do
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

    test "?pg_sql= query param", %{
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
      source =
        insert(:source, user: user)

      {source, backend, cleanup_fn} =
        Logflare.DataCase.setup_clickhouse_test(user: user, source: source)

      on_exit(cleanup_fn)

      start_supervised!({ClickHouseAdaptor, backend})
      assert {:ok, _} = ClickHouseAdaptor.provision_ingest_table(backend)

      message = "query_controller_clickhouse_sql"
      log_event = build(:log_event, source: source, message: message)
      assert :ok = ClickHouseAdaptor.insert_log_events(backend, [log_event])

      query = ~s(select body from "#{source.name}")

      TestUtils.retry_assert(fn ->
        response =
          conn
          |> add_access_token(user, ~w(private))
          |> get(~p"/api/query?#{[sql: query, backend_id: backend.id]}")
          |> json_response(200)

        assert %{"result" => results} = response
        assert Enum.any?(results, fn %{"body" => body} -> body =~ message end)
      end)
    end

    test "?ch_sql= with backend_id", %{conn: conn, user: user} do
      source = insert(:source, user: user)

      {source, backend, cleanup_fn} =
        Logflare.DataCase.setup_clickhouse_test(user: user, source: source)

      on_exit(cleanup_fn)

      start_supervised!({ClickHouseAdaptor, backend})
      assert {:ok, _} = ClickHouseAdaptor.provision_ingest_table(backend)

      message = "query_controller_clickhouse_ch_sql"
      log_event = build(:log_event, source: source, message: message)
      assert :ok = ClickHouseAdaptor.insert_log_events(backend, [log_event])

      query = ~s(select body from "#{source.name}")

      TestUtils.retry_assert(fn ->
        response =
          conn
          |> add_access_token(user, ~w(private))
          |> get(~p"/api/query?#{[ch_sql: query, backend_id: backend.id]}")
          |> json_response(200)

        assert %{"result" => results} = response
        assert Enum.any?(results, fn %{"body" => body} -> body =~ message end)
      end)
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
