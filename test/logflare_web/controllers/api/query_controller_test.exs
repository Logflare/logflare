defmodule LogflareWeb.Api.QueryControllerTest do
  use LogflareWeb.ConnCase

  alias Logflare.Backends.Adaptor.PostgresAdaptor
  setup do
    insert(:plan)
    user = insert(:user)

    {:ok, user: user}
  end

  describe "query with bq" do
    setup do
      stub(Goth, :fetch, fn _mod -> {:ok, %Goth.Token{token: "auth-token"}} end)

      :ok
    end
    test "?sql= query param", %{
      conn: conn,
      user: user,
    } do
      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 2, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"my_time" => "123"}])}
      end)


      response =
        conn
        |> add_access_token(user, ~w(private))
        |> get(~p"/api/query?#{[bq_sql: ~s|select current_datetime() as 'my_time'|]}")
        |> json_response(200)

      assert %{"result"=> [%{"my_time"=> "123"}]} = response

      response =
        conn
        |> recycle()
        |> add_access_token(user, ~w(private))
        |> get(~p"/api/query?#{[sql: ~s|select current_datetime() as 'my_time'|]}")
        |> json_response(200)

      assert %{"result"=> [%{"my_time"=> "123"}]} = response

    end
  end

  describe "pg_sql" do
    setup do
      insert(:plan)

      cfg = Application.get_env(:logflare, Logflare.Repo)

      url = "postgresql://#{cfg[:username]}:#{cfg[:password]}@#{cfg[:hostname]}/#{cfg[:database]}"

      user = insert(:user)
      source = insert(:source, user: user, name: "c")

      source_backend =
        insert(:source_backend,
          type: :postgres,
          config: %{"url" => url},
          source: source
        )

      PostgresAdaptor.create_repo(source_backend)
      assert :ok = PostgresAdaptor.connected?(source_backend)
      PostgresAdaptor.create_log_events_table(source_backend)

      on_exit(fn ->
        PostgresAdaptor.rollback_migrations(source_backend)
        PostgresAdaptor.drop_migrations_table(source_backend)
      end)

      %{source: source, user: user}
    end


    test "?pg_sql= query param", %{
      conn: conn,
      user: user,
    } do

      query = ~S|select now() as "my_time"|
      response =
        conn
        |> add_access_token(user, ~w(private))
        |> get(~p"/api/query?#{[pg_sql: query]}")
        |> json_response(200)

      assert %{"result"=> [%{"my_time"=> _}]} = response
    end
  end

  # describe "show/2" do
  #   test "returns single endpoint query for given user", %{
  #     conn: conn,
  #     user: user,
  #     endpoints: [endpoint | _]
  #   } do
  #     response =
  #       conn
  #       |> add_access_token(user, ~w(private))
  #       |> get("/api/endpoints/#{endpoint.token}")
  #       |> json_response(200)

  #     assert response["token"] == endpoint.token
  #   end

  #   test "returns not found if doesn't own the endpoint query", %{
  #     conn: conn,
  #     endpoints: [endpoint | _]
  #   } do
  #     invalid_user = insert(:user)

  #     conn
  #     |> add_access_token(invalid_user, ~w(private))
  #     |> get("/api/endpoints/#{endpoint.token}")
  #     |> response(404)
  #   end
  # end

  # describe "create/2" do
  #   test "creates a new endpoint query for an authenticated user", %{
  #     conn: conn,
  #     user: user
  #   } do
  #     name = TestUtils.random_string()

  #     response =
  #       conn
  #       |> add_access_token(user, ~w(private))
  #       |> post("/api/endpoints", %{name: name, language: "bq_sql", query: "select a from logs"})
  #       |> json_response(201)

  #     assert response["name"] == name
  #   end

  #   test "returns 422 on missing arguments", %{conn: conn, user: user} do
  #     resp =
  #       conn
  #       |> add_access_token(user, ~w(private))
  #       |> post("/api/endpoints")
  #       |> json_response(422)

  #     assert %{"errors" => %{"name" => _, "query" => _}} = resp
  #   end

  #   test "returns 422 on bad arguments", %{conn: conn, user: user} do
  #     resp =
  #       conn
  #       |> add_access_token(user, ~w(private))
  #       |> post("/api/endpoints", %{name: 123})
  #       |> json_response(422)

  #     assert %{"errors" => %{"name" => _, "query" => _}} = resp
  #   end
  # end

  # describe "update/2" do
  #   test "updates an existing enpoint query from a user", %{
  #     conn: conn,
  #     user: user,
  #     endpoints: [endpoint | _]
  #   } do
  #     name = TestUtils.random_string()

  #     response =
  #       conn
  #       |> add_access_token(user, ~w(private))
  #       |> patch("/api/endpoints/#{endpoint.token}", %{name: name})
  #       |> json_response(204)

  #     assert response["name"] == name
  #   end

  #   test "returns not found if doesn't own the enpoint query", %{
  #     conn: conn,
  #     endpoints: [endpoint | _]
  #   } do
  #     invalid_user = insert(:user)

  #     conn
  #     |> add_access_token(invalid_user, ~w(private))
  #     |> patch("/api/endpoints/#{endpoint.token}", %{name: TestUtils.random_string()})
  #     |> response(404)
  #   end

  #   test "returns 422 on bad arguments", %{
  #     conn: conn,
  #     user: user,
  #     endpoints: [endpoint | _]
  #   } do
  #     resp =
  #       conn
  #       |> add_access_token(user, ~w(private))
  #       |> patch("/api/endpoints/#{endpoint.token}", %{name: 123})
  #       |> json_response(422)

  #     assert resp == %{"errors" => %{"name" => ["is invalid"]}}
  #   end
  # end

  # describe "delete/2" do
  #   test "deletes an existing enpoint query from a user", %{
  #     conn: conn,
  #     user: user,
  #     endpoints: [endpoint | _]
  #   } do
  #     name = TestUtils.random_string()

  #     assert conn
  #            |> add_access_token(user, ~w(private))
  #            |> delete("/api/endpoints/#{endpoint.token}", %{name: name})
  #            |> response(204)

  #     assert conn
  #            |> add_access_token(user, ~w(private))
  #            |> get("/api/endpoints/#{endpoint.token}")
  #            |> response(404)
  #   end

  #   test "returns not found if doesn't own the enpoint query", %{
  #     conn: conn,
  #     endpoints: [endpoint | _]
  #   } do
  #     invalid_user = insert(:user)

  #     assert conn
  #            |> add_access_token(invalid_user, ~w(private))
  #            |> delete("/api/endpoints/#{endpoint.token}", %{name: TestUtils.random_string()})
  #            |> response(404)
  #   end
  # end
end
