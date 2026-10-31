defmodule LogflareWeb.EndpointFilesystemCacheControllerTest do
  use LogflareWeb.ConnCase

  alias Logflare.Backends.Adaptor.QueryResult

  @policy %{
    "start_param" => "from",
    "end_param" => "to",
    "timestamp_format" => "unix_seconds",
    "bypass_fill_above_seconds" => 3600
  }

  setup do
    insert(:plan)
    owner = insert(:user)
    backend = insert(:backend, user: owner, type: :clickhouse)
    %{owner: owner, backend: backend}
  end

  test "invalid windows return actionable HTTP 400 errors with and without result caching",
       context do
    reject(ClickHouseAdaptor, :execute_query, 3)

    for duration <- [0, 60], method <- [:get, :post] do
      endpoint = endpoint(context, duration)

      for params <- [%{"from" => "bad", "to" => "7200"}, %{"from" => "7200", "to" => "0"}] do
        conn =
          Phoenix.ConnTest.dispatch(
            context.conn,
            @endpoint,
            method,
            "/api/endpoints/query/#{endpoint.token}",
            params
          )

        assert %{"error" => message} = json_response(conn, 400)
        assert message =~ "Filesystem cache policy"
        assert message =~ "valid start/end timestamps"
        assert_open_api_response(conn)
      end
    end
  end

  test "valid windows still reach the backend and return query results", context do
    for duration <- [0, 60],
        {finish, settings} <- [
          {"3600", %{}},
          {"7200", %{"read_from_filesystem_cache_if_exists_otherwise_bypass_cache" => 1}}
        ] do
      endpoint = endpoint(context, duration)

      expect(ClickHouseAdaptor, :execute_query, fn _backend, _query, opts ->
        assert opts[:filesystem_cache_settings] == settings
        {:ok, QueryResult.new([%{"ok" => 1}])}
      end)

      conn =
        get(context.conn, "/api/endpoints/query/#{endpoint.token}", %{
          "from" => "0",
          "to" => finish
        })

      assert %{"result" => [%{"ok" => 1}]} = json_response(conn, 200)
    end
  end

  test "invalid saved configuration is not blamed on request timestamps", context do
    endpoint = endpoint(context, 0, Map.put(@policy, "bypass_fill_above_seconds", 0))
    reject(ClickHouseAdaptor, :execute_query, 3)

    conn =
      get(context.conn, "/api/endpoints/query/#{endpoint.token}", %{"from" => "0", "to" => "7200"})

    assert %{"error" => message} = json_response(conn, 500)
    assert message =~ "positive threshold"
    refute message =~ "valid start/end timestamps"
  end

  defp endpoint(context, duration, policy \\ @policy) do
    insert(:endpoint,
      user: context.owner,
      backend: context.backend,
      language: :ch_sql,
      enable_auth: false,
      cache_duration_seconds: duration,
      query: "SELECT @from AS start_time, @to AS end_time",
      filesystem_cache_policy: policy
    )
  end
end
