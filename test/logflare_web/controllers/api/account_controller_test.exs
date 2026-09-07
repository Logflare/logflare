defmodule LogflareWeb.Api.AccountControllerTest do
  use LogflareWeb.ConnCase

  test "returns the authenticated account", %{conn: conn} do
    user = insert(:user)

    response =
      conn
      |> add_access_token(user, "private")
      |> get(~p"/api/account")
      |> json_response(200)

    assert response["token"] == user.token
  end
end
