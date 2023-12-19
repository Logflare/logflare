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
end
