defmodule LogflareWeb.EndpointPolicyAvailabilityTest do
  use LogflareWeb.ConnCase, async: false

  alias Logflare.Endpoints

  test "an unavailable policy returns a retryable service error", %{conn: conn} do
    insert(:plan)
    user = insert(:user)

    endpoint =
      insert(:endpoint,
        user: user,
        language: :ch_sql,
        enable_auth: false,
        cache_duration_seconds: 0
      )

    expect(Endpoints, :get_endpoint_query, fn _id ->
      raise DBConnection.ConnectionError, message: "primary unavailable"
    end)

    conn = get(conn, ~p"/endpoints/query/#{endpoint.token}")
    assert json_response(conn, 503)["error"] =~ "Service Unavailable"
    assert get_resp_header(conn, "retry-after") == ["5"]
  end
end
