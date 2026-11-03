defmodule LogflareWeb.EndpointRunAvailabilityTest do
  use LogflareWeb.ConnCase

  alias Logflare.Backends.QueryError
  alias Logflare.Endpoints
  alias Logflare.Endpoints.Cache
  alias LogflareWeb.QueryErrorHelpers

  setup %{conn: conn} do
    insert(:plan)
    user = insert(:user, endpoints_beta: true)

    backend =
      insert(:backend,
        user: user,
        type: :clickhouse,
        config: %{url: "http://localhost:8123", port: 8123, database: "default"}
      )

    endpoint =
      insert(:endpoint,
        user: user,
        backend: backend,
        language: :ch_sql,
        query: "SELECT 1",
        sandboxable: false
      )

    {:ok, view, _html} =
      conn |> login_user(user) |> live_with_redirect(~p"/endpoints/#{endpoint.id}/edit")

    :ok = Cache.invalidate_endpoint_policy(endpoint.id)
    %{view: view, endpoint: endpoint}
  end

  test "Run reports a primary database blip without crashing", context do
    expect(Endpoints, :get_endpoint_query, fn _id ->
      raise DBConnection.ConnectionError, message: "primary unavailable"
    end)

    html = run_query(context.view)
    assert html =~ "Endpoint policy is temporarily unavailable. Retry the query."
    assert Process.alive?(context.view.pid)

    expect(Endpoints, :run_query_string, fn _, _, _ -> {:ok, %{rows: [%{"n" => 1}]}} end)
    recovered = run_query(context.view)
    assert recovered =~ "Ran query successfully"
    refute recovered =~ "Endpoint policy is temporarily unavailable"
  end

  test "Run reports repeated policy changes without crashing", context do
    expect(Endpoints, :get_endpoint_query, 3, fn id ->
      :ok = Cache.invalidate_endpoint_policy(id)
      context.endpoint
    end)

    html = run_query(context.view)
    assert html =~ "Endpoint policy is temporarily unavailable. Retry the query."
    assert Process.alive?(context.view.pid)
  end

  test "unexpected atom errors use a safe generic message", context do
    expect(Endpoints, :run_query_string, fn _, _, _ -> {:error, :unexpected_internal_state} end)
    html = run_query(context.view)
    assert html =~ QueryErrorHelpers.generic_query_error_message()
    refute html =~ "unexpected_internal_state"
    assert Process.alive?(context.view.pid)
  end

  test "string validation errors retain their message", context do
    expect(Endpoints, :run_query_string, fn _, _, _ -> {:error, "Only SELECT queries allowed"} end)

    assert run_query(context.view) =~ "Only SELECT queries allowed"
    assert Process.alive?(context.view.pid)
  end

  test "structured query errors retain their public description", context do
    error = %QueryError{
      kind: :invalid_query,
      description: "Invalid query column",
      raw_error: %{},
      backend: Logflare.Backends.Adaptor.ClickHouseAdaptor
    }

    expect(Endpoints, :run_query_string, fn _, _, _ -> {:error, error} end)
    assert run_query(context.view) =~ "Invalid query column"
    assert Process.alive?(context.view.pid)
  end

  defp run_query(view),
    do: render_hook(view, "run-query", %{"run" => %{"query" => "SELECT 1"}})
end
