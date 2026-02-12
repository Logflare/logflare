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
    summary: "Create access token",
    request_body: AccessToken.params(),
    responses: %{
      201 => Created.response(AccessToken),
      401 => Unauthorized.response(),
      404 => NotFound.response()
    }
  )

  def create(%{assigns: %{user: user, access_token: current_token}} = conn, params) do
    scopes_list =
      Map.get(params, "scopes", "")
      |> String.split()

    with :ok <- verify_create_scopes(scopes_list, current_token),
         {:ok, access_token} <- Auth.create_access_token(user, params) do
      conn
      |> put_status(201)
      |> json(access_token)
    else
      {:partner_scope, false} -> {:error, :unauthorized}
      {:admin_scope, false} -> {:error, :unauthorized}
      {:error, _} = err -> err
    end
  end

  defp verify_create_scopes(scopes, token) do
    cond do
      "partner" in scopes ->
        {:partner_scope, false}

      Auth.admin_scope() in scopes ->
        if Auth.can_create_admin_token?(token), do: :ok, else: {:admin_scope, false}

      true ->
        :ok
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
