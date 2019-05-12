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
        |> put_req_header("x-api-key", user.api_key)
        |> post("/logs", %{"log_entry" => %{}})

      assert json_response(conn, 403) == %{"message" => "Source or source_name needed."}
    end

    test "succeeds with api_key and source", %{conn: conn, user: user, sources: [s]} do
      conn =
        conn
        |> put_req_header("x-api-key", user.api_key)
        |> post("/logs", %{"log_entry" => %{}, "source" => s.token})

      assert json_response(conn, 200) == %{}
    end
  end
end
