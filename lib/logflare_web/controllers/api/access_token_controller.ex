defmodule LogflareWeb.Api.AccessTokenController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.Auth
  alias LogflareWeb.OpenApi.Accepted
  alias LogflareWeb.OpenApi.Created
  alias LogflareWeb.OpenApi.List
  alias LogflareWeb.OpenApi.NotFound
  alias LogflareWeb.OpenApi.Unauthorized
  alias LogflareWeb.OpenApiSchemas.AccessToken
  alias LogflareWeb.Api.FallbackController

  action_fallback(FallbackController)

  tags(["management"])

  operation(:index,
    summary: "List access tokens",
    responses: %{200 => List.response(AccessToken)}
  )

  def index(%{assigns: %{user: user}} = conn, _) do
    tokens = Auth.list_valid_access_tokens(user) |> Enum.map(&maybe_redact_token/1)
    json(conn, tokens)
  end

  operation(:create,
    summary: "Create source",
    request_body: AccessToken.params(),
    responses: %{
      201 => Created.response(AccessToken),
      401 => Unauthorized.response(),
      404 => NotFound.response()
    }
  )

  def create(%{assigns: %{user: user}} = conn, params) do
    scopes_input = Map.get(params, "scopes", "")

    with {:scopes, true} <- {:scopes, not (scopes_input =~ "partner")},
         {:ok, access_token} <- Auth.create_access_token(user, params) do
      conn
      |> put_status(201)
      |> json(access_token)
    else
      {:scopes, false} ->
        FallbackController.call(conn, {:error, :unauthorized})
    end
  end

  operation(:delete,
    summary: "Delete access token",
    parameters: [token: [in: :path, description: "Access Token", type: :string]],
    responses: %{
      204 => Accepted.response(),
      404 => NotFound.response()
    }
  )

  def delete(%{assigns: %{user: user}} = conn, %{"token" => token}) do
    with access_token <- Auth.get_access_token(user, token),
         {:owner, true} <- {:owner, access_token.resource_owner_id == user.id},
         :ok <- Auth.revoke_access_token(user, token) do
      conn
      |> Plug.Conn.send_resp(204, [])
      |> Plug.Conn.halt()
    else
      {:owner, false} ->
        conn
        |> Plug.Conn.send_resp(404, [])
        |> Plug.Conn.halt()
    end
  end

  defp maybe_redact_token(%{scopes: scopes} = t) do
    cond do
      not (scopes =~ "public") -> %{t | token: nil}
      true -> t
    end
  end
end
