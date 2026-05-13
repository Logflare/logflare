defmodule LogflareWeb.Api.QueryControllerTest do
  use LogflareWeb.ConnCase

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

    test "valid ch_sql query returns 200 ok", %{conn: conn, user: user} do
      conn =
        conn
        |> add_access_token(user, ~w(private))
        |> get(~p"/api/query/parse?#{[ch_sql: ~s|select now() as 'my_time'|]}")

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
  end
end
