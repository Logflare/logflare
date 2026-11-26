defmodule LogflareWeb.Api.EndpointController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  import Logflare.Utils.Guards

  alias Logflare.Backends
  alias Logflare.Endpoints

  alias LogflareWeb.OpenApi.Accepted
  alias LogflareWeb.OpenApi.Created
  alias LogflareWeb.OpenApi.List
  alias LogflareWeb.OpenApi.NotFound
  alias LogflareWeb.OpenApi.UnprocessableEntity

  alias LogflareWeb.OpenApiSchemas.EndpointApiParams
  alias LogflareWeb.OpenApiSchemas.EndpointApiSchema

  action_fallback(LogflareWeb.Api.FallbackController)

  tags(["management"])

  operation(:index,
    summary: "List endpoints",
    responses: %{200 => List.response(EndpointApiSchema)}
  )

  def index(%{assigns: %{user: user}} = conn, _) do
    json(conn, Endpoints.list_endpoints_by(user_id: user.id))
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
    request_body: EndpointApiParams.params(),
    responses: %{
      201 => Created.response(EndpointApiSchema),
      404 => NotFound.response(),
      422 => UnprocessableEntity.response()
    }
  )

  def create(%{assigns: %{user: user}} = conn, params) do
    with :ok <- authorize_backend_id(user, params),
         origin <- conn.assigns[:access_token] || user,
         {:ok, query} <- Endpoints.create_query(user, params, origin) do
      conn
      |> put_status(201)
      |> json(query)
    end
  end

  operation(:update,
    summary: "Update endpoint",
    parameters: [token: [in: :path, description: "Endpoint UUID Token", type: :string]],
    request_body: EndpointApiParams.params(),
    responses: %{
      200 => Accepted.response(EndpointApiSchema),
      204 => Accepted.response(),
      404 => NotFound.response(),
      422 => UnprocessableEntity.response(),
      503 =>
        {"Changes saved, but cache invalidation failed", "application/json",
         %OpenApiSpex.Schema{
           type: :object,
           required: [:saved, :endpoint, :error],
           properties: %{
             saved: %OpenApiSpex.Schema{type: :boolean},
             endpoint: EndpointApiSchema,
             error: %OpenApiSpex.Schema{type: :string}
           }
         }}
    }
  )

  def update(%{assigns: %{user: user}} = conn, %{"token" => token} = params) do
    with query when not is_nil(query) <- Endpoints.get_by(token: token, user_id: user.id),
         :ok <- authorize_backend_id(user, params),
         origin <- conn.assigns[:access_token] || user,
         {:ok, query} <- Endpoints.update_query(user, query, params, origin) do
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
      {:error, {:saved, query, :cache_invalidation_failed}} ->
        conn
        |> put_status(:service_unavailable)
        |> json(%{
          saved: true,
          endpoint: query,
          error: "Changes saved, but cached results could not be invalidated."
        })

      nil ->
        {:error, :not_found}

      err ->
        err
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
    origin = conn.assigns[:access_token] || user

    with query when not is_nil(query) <- Endpoints.get_by(token: token, user_id: user.id),
         {:ok, _} <- Endpoints.delete_query(query, origin) do
      conn
      |> put_status(204)
      |> text("")
    else
      nil -> {:error, :not_found}
      {:error, _} = err -> err
    end
  end

  @spec authorize_backend_id(Logflare.User.t(), map()) :: :ok | {:error, :not_found}
  defp authorize_backend_id(user, params) do
    case params["backend_id"] do
      v when v in [nil, ""] -> :ok
      backend_id -> authorize_cast_backend_id(Ecto.Type.cast(:id, backend_id), user)
    end
  end

  @spec authorize_cast_backend_id({:ok, integer()} | :error, Logflare.User.t()) ::
          :ok | {:error, :not_found}
  defp authorize_cast_backend_id({:ok, id}, user) when is_db_id(id) do
    with {:ok, _backend} <- Backends.fetch_backend_by(id: id, user_id: user.id), do: :ok
  end

  defp authorize_cast_backend_id(_cast, _user), do: {:error, :not_found}
end
