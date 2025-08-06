defmodule LogflareWeb.Api.SourceController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.Sources
  alias Logflare.SourceSchemas
  alias Logflare.Backends
  alias LogflareWeb.OpenApi.Accepted
  alias LogflareWeb.OpenApi.Created
  alias LogflareWeb.OpenApi.List
  alias LogflareWeb.OpenApi.NotFound
  alias LogflareWeb.OpenApiSchemas.Event

  alias LogflareWeb.OpenApiSchemas.Source
  alias LogflareWeb.OpenApiSchemas

  action_fallback(LogflareWeb.Api.FallbackController)

  tags(["management"])

  operation(:index,
    summary: "List sources",
    responses: %{200 => List.response(Source)}
  )

  def index(%{assigns: %{user: user}} = conn, _) do
    sources = Sources.list_sources_by_user(user.id) |> Sources.preload_for_dashboard()
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
    with source when not is_nil(source) <- Sources.get_by(token: token, user_id: user.id) do
      source = Sources.preload_defaults(source)
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
      source = Sources.preload_defaults(source)

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
      204 => Accepted.response(),
      200 => Accepted.response(),
      404 => NotFound.response()
    }
  )

  def update(%{assigns: %{user: user}} = conn, %{"token" => token} = params) do
    with source when not is_nil(source) <- Sources.get_by(token: token, user_id: user.id),
         {:ok, source} <- Sources.update_source_by_user(source, params) do
      source = Sources.preload_defaults(source)

      conn
      |> case do
        %{method: "PATCH"} ->
          conn |> send_resp(204, "")

        %{method: "PUT"} ->
          conn
          |> put_status(200)
          |> json(source)
      end
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

  operation(:recent,
    summary: "Recent events in a source",
    parameters: [token: [in: :path, description: "Source Token", type: :string]],
    responses: %{
      200 => List.response(Event),
      404 => NotFound.response()
    }
  )

  def recent(%{assigns: %{user: user}} = conn, %{"source_token" => token}) do
    with source when not is_nil(source) <- Sources.get_by(token: token, user_id: user.id) do
      recent = for event <- Backends.list_recent_logs(source), do: event.body

      conn
      |> put_status(200)
      |> json(recent)
    end
  end

  operation(:add_backend,
    summary: "Add source backend",
    parameters: [
      source_token: [in: :path, description: "Source Token", type: :string],
      backend_token: [in: :path, description: "Backend Token", type: :string]
    ],
    request_body: nil,
    responses: %{
      201 => Source.response(),
      404 => NotFound.response()
    }
  )

  def add_backend(conn, %{"source_token" => token, "backend_token" => backend_token}) do
    with {:ok, backend} <- Backends.fetch_backend_by(token: backend_token),
         {:ok, source} <- Sources.fetch_source_by(token: token),
         source = Sources.preload_backends(source),
         {:ok, source} <- Backends.update_source_backends(source, [backend | source.backends]) do
      conn
      |> put_status(201)
      |> json(source)
    end
  end

  operation(:remove_backend,
    summary: "Remove source backend",
    parameters: [
      source_token: [in: :path, description: "Source Token", type: :string],
      backend_token: [in: :path, description: "Backend Token", type: :string]
    ],
    responses: %{
      200 => Source.response(),
      404 => NotFound.response()
    }
  )

  def remove_backend(conn, %{"source_token" => token, "backend_token" => backend_token}) do
    with {:ok, source} <- Sources.fetch_source_by(token: token),
         source = Sources.preload_backends(source),
         filtered = Enum.filter(source.backends, &(&1.token != backend_token)),
         {:ok, source} <- Backends.update_source_backends(source, filtered) do
      conn
      |> put_status(200)
      |> json(source)
    end
  end

  operation(:show_schema,
    summary: "Show source schema",
    parameters: [token: [in: :path, description: "Source Token", type: :string]],
    responses: %{
      200 => OpenApiSchemas.SourceSchema.response(),
      404 => NotFound.response()
    }
  )

  def show_schema(%{assigns: %{user: user}} = conn, %{"source_token" => token} = params) do
    with source when not is_nil(source) <- Sources.get_by(token: token, user_id: user.id) do
      schema = SourceSchemas.Cache.get_source_schema_by(source_id: source.id)

      data =
        if Map.get(params, "variant") == "dot" do
          SourceSchemas.format_schema(schema, :dot)
        else
          SourceSchemas.format_schema(schema, :json_schema, %{
            :title => source.name,
            :"$id" => ~p"/sources/#{source.token}/schema"
          })
        end

      json(conn, data)
    end
  end
end
