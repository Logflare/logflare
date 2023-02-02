defmodule LogflareWeb.Api.AccountController do
  use LogflareWeb, :controller
  alias Logflare.Users
  action_fallback LogflareWeb.Api.FallbackController

  def create(conn, params) do
    token = Ecto.UUID.generate()

    params =
      Map.merge(params, %{
        "provider_uid" => Ecto.UUID.generate(),
        "provider" => "email",
        "token" => token
      })

    with {:ok, user} <- Users.insert_user(params) do
      conn
      |> put_status(201)
      |> json(%{user: user, token: token})
    end
  end
end
