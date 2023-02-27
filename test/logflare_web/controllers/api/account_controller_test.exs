defmodule LogflareWeb.Api.AccountControllerTest do
  use LogflareWeb.ConnCase

  setup do
    [token: token] =
      Application.get_env(:logflare, LogflareWeb.Plugs.EnsureSuperUserAuthentication)

    {:ok, %{token: token}}
  end

  describe "create/2" do
    test "returns 201 and the user information and a token to access the API", %{
      conn: conn,
      token: token
    } do
      email = TestUtils.gen_email() |> String.downcase()

      response =
        conn
        |> put_req_header("authorization", "Bearer #{token}")
        |> post("/api/accounts", %{email: email})
        |> json_response(201)

      assert response["user"]["email"] == email
      assert response["token"]
    end

    test "returns 400 when no email is given", %{conn: conn, token: token} do
      assert conn
             |> put_req_header("authorization", "Bearer #{token}")
             |> post("/api/accounts")
             |> json_response(422)
    end

    test "return 403 with the wrong token", %{conn: conn} do
      assert conn
             |> put_req_header("authorization", "Bearer potato")
             |> post("/api/accounts")
             |> json_response(401) == %{"message" => "Error, invalid token!"}
    end
  end
end
