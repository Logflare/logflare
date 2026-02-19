defmodule LogflareWeb.Api.BackendController do
  alias LogflareWeb.OpenApiSchemas.BackendConnectionTest
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.Backends
  alias LogflareWeb.OpenApi.Accepted
  alias LogflareWeb.OpenApi.Created
  alias LogflareWeb.OpenApi.List
  alias LogflareWeb.OpenApi.NotFound
  alias LogflareWeb.OpenApiSchemas.BackendApiSchema

  action_fallback(LogflareWeb.Api.FallbackController)

  tags(["management"])

  operation(:index,
    summary: "List backends. Default managed backends are not included.",
    responses: %{200 => List.response(BackendApiSchema)}
  )

  def index(%{assigns: %{user: user}} = conn, params) do
    backends = Backends.list_backends(user_id: user.id, metadata: params["metadata"])
    json(conn, backends)
  end

  operation(:show,
    summary: "Fetch backend",
    parameters: [token: [in: :path, description: "Backend token", type: :string]],
    responses: %{
      200 => BackendApiSchema.response(),
      404 => NotFound.response()
    }
  )

  def show(%{assigns: %{user: user}} = conn, %{"token" => token}) do
    with {:ok, backend} <- Backends.fetch_backend_by(token: token, user_id: user.id) do
      json(conn, backend)
    end
  end

  operation(:create,
    summary: "Create backend",
    request_body: BackendApiSchema.params(),
    responses: %{
      201 => Created.response(BackendApiSchema),
      404 => NotFound.response()
    }
  )

  def create(%{assigns: %{user: user}} = conn, params) do
    params = Map.put(params, "user_id", user.id)

    with {:ok, backend} <- Backends.create_backend(params) do
      conn
      |> put_status(201)
      |> json(backend)
    end
  end

  operation(:update,
    summary: "Update backend",
    parameters: [token: [in: :path, description: "Backend Token", type: :string]],
    request_body: BackendApiSchema.params(),
    responses: %{
      204 => Accepted.response(),
      200 => Accepted.response(BackendApiSchema),
      404 => NotFound.response()
    }
  )

  def update(%{assigns: %{user: user}} = conn, %{"token" => token} = params) do
    with {:ok, backend} <- Backends.fetch_backend_by(token: token, user_id: user.id),
         {:ok, updated} <- Backends.update_backend(backend, params) do
      conn
      |> case do
        %{method: "PATCH"} ->
          conn
          |> send_resp(204, "")

        %{method: "PUT"} ->
          put_status(conn, 200)
          |> json(updated)
      end
    end
  end

  operation(:delete,
    summary: "Delete backend",
    parameters: [token: [in: :path, description: "Backend Token", type: :string]],
    responses: %{
      204 => Accepted.response(),
      404 => NotFound.response()
    }
  )

  def delete(%{assigns: %{user: user}} = conn, %{"token" => token}) do
    with {:ok, backend} <- Backends.fetch_backend_by(token: token, user_id: user.id),
         {:ok, _} <- Backends.delete_backend(backend) do
      conn
      |> Plug.Conn.send_resp(204, [])
      |> Plug.Conn.halt()
    end
  end

  operation(:test_connection,
    summary: "Test backend connection",
    parameters: [token: [in: :path, description: "Backend Token", type: :string]],
    responses: %{
      200 => BackendConnectionTest.response(),
      404 => NotFound.response()
    }
  )

  def test_connection(%{assigns: %{user: user}} = conn, %{"token" => token}) do
    with {:ok, backend} <- Backends.fetch_backend_by(token: token, user_id: user.id) do
      case Backends.test_connection(backend) do
        :ok ->
          conn
          |> json(%{connected?: true})

        {:error, reason} ->
          conn
          |> json(%{connected?: false, reason: reason})
      end
    end
  end
end
