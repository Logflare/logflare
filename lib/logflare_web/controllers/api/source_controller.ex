defmodule LogflareWeb.Api.SourceController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.Sources
  alias Logflare.Users

  alias LogflareWeb.OpenApi.Accepted
  alias LogflareWeb.OpenApi.Created
  alias LogflareWeb.OpenApi.List
  alias LogflareWeb.OpenApi.NotFound

  alias LogflareWeb.OpenApiSchemas.Source

  action_fallback(LogflareWeb.Api.FallbackController)

  tags(["management"])

  operation(:index,
    summary: "List sources",
    responses: %{200 => List.response(Source)}
  )

  def index(%{assigns: %{user: user}} = conn, _) do
    user = Users.preload_sources(user)
    sources = Sources.preload_for_dashboard(user.sources)
    json(conn, sources)
  end

  operation(:show,
    summary: "Fetch source",
    parameters: [token: [in: :path, description: "Source Token", type: :string]],
    responses: %{
      200 => Source.response(),
      404 => NotFound.response()
    }
  )

  def show(%{assigns: %{user: user}} = conn, %{"token" => token}) do
    with source when not is_nil(source) <- Sources.get_by(token: token, user_id: user.id),
         [source] <- Sources.preload_for_dashboard([source]) do
      json(conn, source)
    end
  end

  operation(:create,
    summary: "Create source",
    request_body: Source.params(),
    responses: %{
      201 => Created.response(Source),
      404 => NotFound.response()
    }
  )

  def create(%{assigns: %{user: user}} = conn, params) do
    with {:ok, source} <- Sources.create_source(params, user) do
      conn
      |> put_status(201)
      |> json(source)
    end
  end

  operation(:update,
    summary: "Update source",
    parameters: [token: [in: :path, description: "Source Token", type: :string]],
    request_body: Source.params(),
    responses: %{
      201 => Created.response(Source),
      404 => NotFound.response()
    }
  )

  def update(%{assigns: %{user: user}} = conn, %{"token" => token} = params) do
    with source when not is_nil(source) <- Sources.get_by(token: token, user_id: user.id),
         {:ok, source} <- Sources.update_source_by_user(source, params) do
      conn
      |> put_status(204)
      |> json(source)
    end
  end

  operation(:delete,
    summary: "Delete source",
    parameters: [token: [in: :path, description: "Source Token", type: :string]],
    responses: %{
      204 => Accepted.response(),
      404 => NotFound.response()
    }
  )

  def delete(%{assigns: %{user: user}} = conn, %{"token" => token}) do
    with source when not is_nil(source) <- Sources.get_by(token: token, user_id: user.id),
         {:ok, _} <- Sources.delete_source(source) do
      conn
      |> Plug.Conn.send_resp(204, [])
      |> Plug.Conn.halt()
    end
  end
end
