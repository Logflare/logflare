defmodule LogflareWeb.Api.EndpointControllerTest do
  use LogflareWeb.ConnCase
  import Logflare.Factory

  setup do
    endpoints = insert_list(2, :endpoint)
    user = insert(:user, endpoint_queries: endpoints)

    {:ok, user: user, endpoints: endpoints}
  end

  describe "index/2" do
    test "returns list of sources for given user", %{
      conn: conn,
      user: user,
      endpoints: endpoints
    } do
      response =
        conn
        |> login_user(user)
        |> get("/api/endpoints")
        |> json_response(200)

      response = response |> Enum.map(& &1["token"]) |> Enum.sort()
      expected = endpoints |> Enum.map(& &1.token) |> Enum.sort()

      assert response == expected
    end
  end

  describe "show/2" do
    test "returns single sources for given user", %{
      conn: conn,
      user: user,
      endpoints: [endpoint | _]
    } do
      response =
        conn
        |> login_user(user)
        |> get("/api/endpoints/#{endpoint.token}")
        |> json_response(200)

      assert response["token"] == endpoint.token
    end

    test "returns not found if doesn't own the source", %{
      conn: conn,
      endpoints: [endpoint | _]
    } do
      invalid_user = insert(:user)

      conn
      |> login_user(invalid_user)
      |> get("/api/endpoints/#{endpoint.token}")
      |> response(404)
    end
  end
end
