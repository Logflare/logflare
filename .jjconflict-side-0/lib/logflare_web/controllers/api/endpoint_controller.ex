defmodule LogflareWeb.Api.EndpointController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.Users
  alias Logflare.Endpoints

  alias LogflareWeb.OpenApi.Accepted
  alias LogflareWeb.OpenApi.Created
  alias LogflareWeb.OpenApi.List
  alias LogflareWeb.OpenApi.NotFound
  alias LogflareWeb.OpenApi.UnprocessableEntity

  alias LogflareWeb.OpenApiSchemas.EndpointApiSchema

  action_fallback(LogflareWeb.Api.FallbackController)

  tags(["management"])

  operation(:index,
    summary: "List endpoints",
    responses: %{200 => List.response(EndpointApiSchema)}
  )

  def index(%{assigns: %{user: user}} = conn, _) do
    user = Users.preload_endpoints(user)
    json(conn, user.endpoint_queries)
  end

  operation(:show,
    summary: "Fetch endpoint",
    parameters: [token: [in: :path, description: "Endpoint UUID Token", type: :string]],
    responses: %{
      200 => EndpointApiSchema.response(),
      404 => NotFound.response()
    }
  )

  def show(%{assigns: %{user: user}} = conn, %{"token" => token}) do
    with query when not is_nil(query) <- Endpoints.get_by(token: token, user_id: user.id) do
      json(conn, query)
    else
      nil -> {:error, :not_found}
    end
  end

  operation(:create,
    summary: "Create endpoint",
    request_body: EndpointApiSchema.params(),
    responses: %{
      201 => Created.response(EndpointApiSchema),
      404 => NotFound.response(),
      422 => UnprocessableEntity.response()
    }
  )

  def create(%{assigns: %{user: user}} = conn, params) do
    with {:ok, query} <- Endpoints.create_query(user, params) do
      conn
      |> put_status(201)
      |> json(query)
    else
      nil -> {:error, :not_found}
      err -> err
    end
  end

  operation(:update,
    summary: "Update endpoint",
    parameters: [token: [in: :path, description: "Endpoint UUID Token", type: :string]],
    request_body: EndpointApiSchema.params(),
    responses: %{
      200 => Accepted.response(EndpointApiSchema),
      204 => Accepted.response(),
      404 => NotFound.response(),
      422 => UnprocessableEntity.response()
    }
  )

  def update(%{assigns: %{user: user}} = conn, %{"token" => token} = params) do
    with query when not is_nil(query) <- Endpoints.get_by(token: token, user_id: user.id),
         {:ok, query} <- Endpoints.update_query(query, params) do
      conn
      |> case do
        %{method: "PUT"} ->
          conn
          |> put_status(200)
          |> json(query)

        %{method: "PATCH"} ->
          conn
          |> put_status(204)
          |> text("")
      end
    else
      nil -> {:error, :not_found}
      err -> err
    end
  end

  tags(["management"])

  operation(:delete,
    summary: "Delete endpoint",
    parameters: [token: [in: :path, description: "Endpoint UUID Token", type: :string]],
    responses: %{
      204 => Accepted.response(),
      404 => NotFound.response()
    }
  )

  def delete(%{assigns: %{user: user}} = conn, %{"token" => token}) do
    with query when not is_nil(query) <- Endpoints.get_by(token: token, user_id: user.id),
         {:ok, _} <- Endpoints.delete_query(query) do
      conn
      |> put_status(204)
      |> text("")
    else
      nil -> {:error, :not_found}
      {:error, _} = err -> err
    end
  end
end
