defmodule LogflareWeb.EndpointsControllerTest do
  use LogflareWeb.ConnCase, mock_sql: true

  setup do
    source = build(:source, rules: [])
    user = insert(:user, sources: [source])
    {:ok, user: user, source: source}
  end

  describe "query" do
    test "GET query", %{conn: init_conn, user: user} do
      endpoint = insert(:endpoint, user: user, enable_auth: false)

        init_conn
        |> get("/endpoints/query/#{endpoint.token}")
        |> assert_valid

        init_conn
        |> get("/api/endpoints/query/#{endpoint.token}")
        |> assert_valid
    end
  end
  defp assert_valid(conn) do
    assert conn.halted == false
    assert html_response(conn, 200)
    conn
  end
end
