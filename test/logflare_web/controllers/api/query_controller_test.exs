defmodule LogflareWeb.Api.QueryControllerTest do
  use LogflareWeb.ConnCase

  alias Logflare.Backends.Adaptor
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

  describe "query with promql" do
    setup %{user: user} do
      backend =
        insert(:backend,
          user: user,
          type: :victoria_metrics,
          config: %{
            url: "https://8.8.8.8/api/v1/write",
            query_url: "https://8.8.8.8",
            username: "query-user",
            password: "query:password"
          }
        )

      stub(Tesla.Adapter.Finch, :call, fn _env, _opts ->
        flunk("Unexpected VictoriaMetrics request")
      end)

      %{backend: backend}
    end

    test "forwards raw expressions and instant options with stored basic authentication", %{
      conn: conn,
      user: user,
      backend: backend
    } do
      query = ~S'sum(rate(http_requests_total{path=~"/a|b",service="café"}[5m])) by (job)'
      data = %{"resultType" => "vector", "result" => []}
      expected = %{"status" => "success", "data" => data, "warnings" => ["partial data"]}

      expect(Tesla.Adapter.Finch, :call, fn env, _opts ->
        assert env.method == :post
        assert env.url == "https://8.8.8.8/api/v1/query"

        assert Tesla.get_header(env, "authorization") ==
                 "Basic " <> Base.encode64("query-user:query:password")

        assert URI.decode_query(env.body) == %{
                 "query" => query,
                 "time" => "2026-10-08T01:02:03Z",
                 "timeout" => "10s"
               }

        tesla_response(env, 200, expected)
      end)

      assert promql_response(conn, user, %{
               promql: query,
               backend_id: backend.id,
               time: "2026-10-08T01:02:03Z",
               timeout: "10s",
               query: "ignored",
               username: "ignored",
               password: "ignored"
             }) == expected
    end

    test "range queries preserve timestamps, labels, and special sample values", %{
      conn: conn,
      user: user,
      backend: backend
    } do
      expected = %{
        "status" => "success",
        "data" => %{
          "resultType" => "matrix",
          "result" => [
            %{
              "metric" => %{"__name__" => "temperature", "instance" => "host-1"},
              "values" => [[1_791_421_200.25, "NaN"], [1_791_421_215.25, "+Inf"]]
            }
          ]
        },
        "infos" => ["sample information"]
      }

      expect(Tesla.Adapter.Finch, :call, fn env, _opts ->
        assert env.url == "https://8.8.8.8/api/v1/query_range"

        assert URI.decode_query(env.body) == %{
                 "query" => "temperature",
                 "start" => "1791421200.25",
                 "end" => "1791421260.25",
                 "step" => "15s"
               }

        tesla_response(env, 200, expected)
      end)

      assert promql_response(conn, user, %{
               promql: "temperature",
               backend_id: backend.id,
               start: "1791421200.25",
               end: "1791421260.25",
               step: "15s"
             }) == expected
    end

    test "preserves vector, scalar, and string response shapes", %{
      conn: conn,
      user: user,
      backend: backend
    } do
      for {type, result} <- [
            {"vector", [%{"metric" => %{"job" => "api"}, "value" => [123.5, "-Inf"]}]},
            {"scalar", [123.5, "NaN"]},
            {"string", [123.5, "a string result"]}
          ] do
        expected = %{"status" => "success", "data" => %{"resultType" => type, "result" => result}}

        expect(Tesla.Adapter.Finch, :call, fn env, _opts ->
          tesla_response(env, 200, expected)
        end)

        assert promql_response(conn, user, %{promql: "up", backend_id: backend.id}) == expected
      end
    end

    test "preserves native backend errors and HTTP statuses", %{
      conn: conn,
      user: user,
      backend: backend
    } do
      for {status, type} <- [{400, "bad_data"}, {422, "execution"}, {503, "timeout"}] do
        expected = %{
          "status" => "error",
          "errorType" => type,
          "error" => "query failed",
          "warnings" => ["backend warning"]
        }

        expect(Tesla.Adapter.Finch, :call, fn env, _opts ->
          tesla_response(env, status, expected)
        end)

        assert promql_response(conn, user, %{promql: "up", backend_id: backend.id}, status) ==
                 expected
      end
    end

    test "requires an explicit valid backend ID", %{conn: conn, user: user} do
      for params <- [%{promql: "up"}, %{promql: "up", backend_id: ""}] do
        assert %{
                 "status" => "error",
                 "errorType" => "bad_data",
                 "error" => "backend_id is required for PromQL queries"
               } =
                 promql_response(conn, user, params, 400)
      end

      for backend_id <- ["invalid", "1.5", %{id: "1"}] do
        assert %{"error" => "Invalid backend_id: must be an integer"} =
                 promql_response(conn, user, %{promql: "up", backend_id: backend_id}, 400)
      end

      for backend_id <- [0, -1, "9223372036854775808"] do
        assert %{"error" => "Invalid backend_id: must be a positive 64-bit integer"} =
                 promql_response(conn, user, %{promql: "up", backend_id: backend_id}, 400)
      end

      assert %{"error" => "Backend not found"} =
               promql_response(conn, user, %{promql: "up", backend_id: 999_999}, 400)
    end

    test "rejects another user's backend and non-VictoriaMetrics backends", %{
      conn: conn,
      user: user
    } do
      other_backend = insert(:backend, user: insert(:user), type: :victoria_metrics)
      sql_backend = insert(:backend, user: user, type: :clickhouse)

      assert %{"error" => "Backend not found"} =
               promql_response(conn, user, %{promql: "up", backend_id: other_backend.id}, 400)

      assert %{"error" => "Backend does not support PromQL queries"} =
               promql_response(conn, user, %{promql: "up", backend_id: sql_backend.id}, 400)
    end

    test "rejects every SQL parameter mixed with PromQL", %{
      conn: conn,
      user: user,
      backend: backend
    } do
      for sql_key <- [:sql, :bq_sql, :ch_sql, :pg_sql] do
        params = Map.put(%{promql: "up", backend_id: backend.id}, sql_key, "SELECT 1")

        assert %{"error" => "promql cannot be combined with SQL parameters"} =
                 promql_response(conn, user, params, 400)
      end
    end

    test "rejects malformed expressions and options before sending a request", %{
      conn: conn,
      user: user,
      backend: backend
    } do
      for invalid <- [
            %{promql: " "},
            %{promql: %{query: "up"}},
            %{time: %{invalid: "value"}},
            %{start: "1"},
            %{start: "1", end: "2", step: "1s", time: "1"}
          ] do
        params = Map.merge(%{promql: "up", backend_id: backend.id}, invalid)

        assert %{"status" => "error", "errorType" => "bad_data", "error" => error} =
                 promql_response(conn, user, params, 400)

        assert is_binary(error)
      end
    end

    test "VictoriaMetrics remains unavailable to SQL execution and parsing", %{
      conn: conn,
      user: user,
      backend: backend
    } do
      refute Adaptor.can_query?(backend)
      params = %{sql: "SELECT 1", backend_id: backend.id}

      assert %{"error" => "Backend does not support querying"} =
               promql_response(conn, user, params, 400)

      response =
        conn
        |> add_access_token(user, ~w(private))
        |> get(~p"/api/query/parse", params)
        |> json_response(400)

      assert %{"error" => "Backend does not support querying"} = response
    end

    test "requires management authentication", %{conn: conn, backend: backend} do
      conn = get(conn, ~p"/api/query", %{promql: "up", backend_id: backend.id})
      assert %{"error" => _error} = json_response(conn, 401)
    end
  end

  @spec promql_response(Plug.Conn.t(), Logflare.User.t(), map(), pos_integer()) :: map()
  defp promql_response(conn, user, params, status \\ 200) do
    conn
    |> add_access_token(user, ~w(private))
    |> get(~p"/api/query", params)
    |> json_response(status)
  end

  @spec tesla_response(Tesla.Env.t(), pos_integer(), map()) :: Tesla.Env.result()
  defp tesla_response(%Tesla.Env{} = env, status, body) do
    {:ok,
     %Tesla.Env{
       env
       | status: status,
         headers: [{"content-type", "application/json"}],
         body: Jason.encode!(body)
     }}
  end
end
