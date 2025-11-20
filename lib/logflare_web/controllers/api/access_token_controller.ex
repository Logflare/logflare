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

    with {:scopes, true} <- {:scopes, "partner" not in String.split(scopes_input)},
         {:ok, access_token} <- Auth.create_access_token(user, params) do
      conn
      |> put_status(201)
      |> json(access_token)
    else
      {:scopes, false} -> {:error, :unauthorized}
      {:error, _} = err -> err
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
    with {:get, %_{resource_owner_id: resource_owner_id}} <-
           {:get, Auth.get_access_token(user, token)},
         {:owner, true} <- {:owner, resource_owner_id == user.id},
         :ok <- Auth.revoke_access_token(user, token) do
      Plug.Conn.send_resp(conn, 204, [])
    else
      {:get, nil} -> {:error, :not_found}
      # don't reveal that the token exists
      {:owner, false} -> {:error, :not_found}
      {:error, _} = err -> err
    end
  end

  defp maybe_redact_token(%{scopes: scopes} = t) do
    if "public" in String.split(scopes), do: t, else: %{t | token: nil}
  end
end
