defmodule LogflareWeb.LogsRouteTest do
  use LogflareWeb.ConnCase
  alias Logflare.TableManager
  import Logflare.DummyFactory

  setup do
    :ok = Ecto.Adapters.SQL.Sandbox.mode(Logflare.Repo, {:shared, self()})
    s = insert(:source, token: Faker.UUID.v4())
    Logflare.TableCounter.start_link()
    Logflare.SystemCounter.start_link()
    TableManager.start_link([s.token])
    u = insert(:user, api_key: Faker.String.base64(), sources: [s])
    {:ok, user: u, sources: [s]}
  end

  describe "POST /logs" do
    test "fails without api key", %{conn: conn, user: user} do
      conn = post(conn, "/logs", %{"log_entry" => %{}})
      assert json_response(conn, 403) == %{"message" => "Unknown x-api-key."}
    end

    test "fails without source or source_name", %{conn: conn, user: user} do
      conn =
        conn
        |> put_api_key_header(user.api_key)
        |> post("/logs", %{"log_entry" => %{}})

      assert json_response(conn, 403) == %{"message" => "Source or source_name needed."}
    end

    test "fails with an empty-ish log_entry", %{conn: conn, user: u, sources: [s]} do
      conn1 = post_logs(conn, u, s, %{})

      assert json_response(conn1, 403) == %{"message" => "Log entry needed."}

      conn2 = post_logs(conn, u, s, nil)

      assert json_response(conn2, 403) == %{"message" => "Log entry needed."}

      conn3 = post_logs(conn, u, s, [])

      assert json_response(conn3, 403) == %{"message" => "Log entry needed."}
    end
  end

  def post_logs(conn, user, source, log_entry) do
    conn
    |> put_api_key_header(user.api_key)
    |> post("/logs", %{"log_entry" => [], "source" => Atom.to_string(source.token)})
  end

  def put_api_key_header(conn, api_key) do
    conn
    |> put_req_header("x-api-key", api_key)
  end
end
