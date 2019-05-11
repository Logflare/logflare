defmodule LogflareWeb.LogsRouteTest do
  use LogflareWeb.ConnCase
  import Logflare.DummyFactory

  setup do
    :ok = Ecto.Adapters.SQL.Sandbox.mode(Logflare.Repo, {:shared, self()})
    u = insert(:user, api_key: Faker.String.base64())
    {:ok, user: u}
  end

  describe "POST /logs" do
    test "without api key", %{conn: conn, user: user} do

      conn = post(conn, "/logs", %{"log_entry" => %{}})
      assert json_response(conn, 403) == %{"message" => "Unknown x-api-key."}
    end
  end
end
