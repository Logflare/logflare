defmodule LogflareWeb.Api.EndpointController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.Users
  alias Logflare.Endpoints

  alias LogflareWeb.OpenApi.Accepted
  alias LogflareWeb.OpenApi.Created
  alias LogflareWeb.OpenApi.List
  alias LogflareWeb.OpenApi.NotFound

  alias LogflareWeb.OpenApiSchemas.Endpoint

  action_fallback(LogflareWeb.Api.FallbackController)

  tags(["management"])

  operation(:index,
    summary: "List endpoints",
    responses: %{200 => List.response(Endpoint)}
  )

  def index(%{assigns: %{user: user}} = conn, _) do
    user = Users.preload_endpoints(user)
    json(conn, user.endpoint_queries)
  end

  operation(:show,
    summary: "Fetch endpoint",
    parameters: [token: [in: :path, description: "Endpoint Token", type: :string]],
    responses: %{
      200 => Endpoint.response(),
      404 => NotFound.response()
    }
  )

  def show(%{assigns: %{user: user}} = conn, %{"token" => token}) do
    with query when not is_nil(query) <- Endpoints.get_by(token: token, user_id: user.id) do
      json(conn, query)
    end
  end

  operation(:create,
    summary: "Create endpoint",
    request_body: Endpoint.params(),
    responses: %{
      201 => Created.response(Endpoint),
      404 => NotFound.response()
    }
  )

  def create(%{assigns: %{user: user}} = conn, params) do
    with {:ok, query} <- Endpoints.create_query(user, params) do
      conn
      |> put_status(201)
      |> json(query)
    end
  end

  operation(:update,
    summary: "Update endpoint",
    parameters: [token: [in: :path, description: "Endpoint Token", type: :string]],
    request_body: Endpoint.params(),
    responses: %{
      204 => Accepted.response(),
      201 => Created.response(Endpoint),
      404 => NotFound.response()
    }
  )

  def update(%{assigns: %{user: user}} = conn, %{"token" => token} = params) do
    with query when not is_nil(query) <- Endpoints.get_by(token: token, user_id: user.id),
         {:ok, query} <- Endpoints.update_query(query, params) do
      conn
      |> case do
        %{method: "PUT"} -> put_status(conn, 201)
        %{method: "PATCH"} -> put_status(conn, 204)
      end
      |> json(query)
    end
  end

  tags(["management"])

  operation(:delete,
    summary: "Delete endpoint",
    parameters: [token: [in: :path, description: "Endpoint Token", type: :string]],
    responses: %{
      204 => Accepted.response(),
      404 => NotFound.response()
    }
  )

  def delete(%{assigns: %{user: user}} = conn, %{"token" => token}) do
    with query when not is_nil(query) <- Endpoints.get_by(token: token, user_id: user.id),
         {:ok, _} <- Endpoints.delete_query(query) do
      conn
      |> Plug.Conn.send_resp(204, [])
      |> Plug.Conn.halt()
    end
  end
end
