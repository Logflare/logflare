defmodule LogflareWeb.Api.AccessTokenController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.Auth
  alias LogflareWeb.OpenApi.Accepted
  alias LogflareWeb.OpenApi.Created
  alias LogflareWeb.OpenApi.List
  alias LogflareWeb.OpenApi.NotFound
  alias LogflareWeb.OpenApi.Unauthorized
  alias LogflareWeb.OpenApi.UnprocessableEntity
  alias LogflareWeb.OpenApiSchemas.AccessToken
  alias LogflareWeb.Api.FallbackController

  action_fallback(FallbackController)

  tags(["management"])

  operation(:index,
    summary: "List access tokens",
    responses: %{200 => List.response(AccessToken)}
  )

  def index(%{assigns: %{user: user}} = conn, _) do
    tokens =
      user
      |> Auth.list_valid_access_tokens()
      |> Enum.map(&maybe_redact_token/1)
      |> Enum.map(&format_access_token/1)

    json(conn, tokens)
  end

  operation(:create,
    summary: "Create access token",
    request_body: AccessToken.params(),
    responses: %{
      201 => Created.response(AccessToken),
      401 => Unauthorized.response(),
      404 => NotFound.response(),
      422 => UnprocessableEntity.response()
    }
  )

  def create(%{assigns: %{user: user, access_token: current_token}} = conn, params) do
    attrs = Map.take(params, ["description", "scopes"])

    with {:ok, access_token} <- Auth.create_access_token(current_token, user, attrs) do
      conn
      |> put_status(201)
      |> json(format_access_token(access_token))
    else
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

  defp format_access_token(%{inserted_at: inserted_at} = token) do
    %{token | inserted_at: DateTime.from_naive!(inserted_at, "Etc/UTC")}
  end
end
