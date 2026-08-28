defmodule LogflareWeb.Api.SourceController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.Backends
  alias Logflare.Partners.Partner
  alias Logflare.Sources
  alias Logflare.SourceSchemas
  alias LogflareWeb.Api.FallbackController
  alias LogflareWeb.Api.SourceCsv
  alias LogflareWeb.OpenApi.Accepted
  alias LogflareWeb.OpenApi.Created
  alias LogflareWeb.OpenApi.List
  alias LogflareWeb.OpenApi.NotFound
  alias LogflareWeb.OpenApi.Unauthorized
  alias LogflareWeb.OpenApi.UnprocessableEntity
  alias LogflareWeb.OpenApiSchemas
  alias LogflareWeb.OpenApiSchemas.Event
  alias LogflareWeb.OpenApiSchemas.Source
  alias LogflareWeb.OpenApiSchemas.SourceIndexResponse
  alias LogflareWeb.OpenApiSchemas.SourceParams
  alias OpenApiSpex.MediaType
  alias OpenApiSpex.Response
  alias OpenApiSpex.Schema

  action_fallback(LogflareWeb.Api.FallbackController)

  @max_source_id 9_223_372_036_854_775_807

  tags(["management"])

  operation(:index,
    summary: "List sources",
    description:
      "Private tokens receive the full management source representation as JSON. Ingest-compatible credentials receive only token and name. Request text/csv for a minimal token,name CSV list.",
    responses: %{
      200 => %Response{
        description: "Source list",
        content: %{
          "application/json" => %MediaType{schema: SourceIndexResponse},
          "text/csv" => %MediaType{schema: %Schema{type: :string}}
        }
      },
      401 => Unauthorized.response()
    }
  )

  def index(%{assigns: %{user: user}} = conn, _params) do
    case {get_format(conn), source_access_for_conn(conn)} do
      {_format, :unauthorized} ->
        FallbackController.call(conn, {:error, :unauthorized})

      {"csv", access} ->
        conn
        |> put_resp_header("cache-control", "no-store")
        |> put_resp_content_type("text/csv")
        |> send_resp(
          200,
          SourceCsv.encode(Sources.list_source_tokens_by_user(user.id, source_ids(access)))
        )

      {"json", :private} ->
        render_private_json(conn, user.id)

      {"json", :partner} ->
        render_private_json(conn, user.id)

      {"json", {:ingest, source_ids}} ->
        render_minimal_json(conn, user.id, source_ids)
    end
  end

  defp render_private_json(conn, user_id) do
    conn
    |> put_resp_header("cache-control", "no-store")
    |> json(Sources.list_sources_by_user(user_id) |> Sources.preload_for_dashboard())
  end

  defp render_minimal_json(conn, user_id, source_ids) do
    conn
    |> put_resp_header("cache-control", "no-store")
    |> json(Sources.list_source_tokens_by_user(user_id, source_ids))
  end

  defp source_access_for_conn(%{assigns: %{partner: %Partner{}}}), do: :partner

  defp source_access_for_conn(%{assigns: %{access_token: access_token}}),
    do: source_access_for_token(access_token)

  defp source_access_for_conn(_conn), do: {:ingest, nil}

  defp source_ids(:private), do: nil
  defp source_ids(:partner), do: nil
  defp source_ids({:ingest, source_ids}), do: source_ids

  defp source_access_for_token(nil), do: {:ingest, nil}

  defp source_access_for_token(%{scopes: scopes}) do
    scopes = String.split(scopes || "")

    cond do
      "private" in scopes -> :private
      scopes == [] or "public" in scopes or "ingest" in scopes -> {:ingest, nil}
      true -> scoped_source_access(scopes)
    end
  end

  defp scoped_source_access(scopes) do
    source_ids =
      scopes
      |> Enum.flat_map(fn
        "ingest:source:" <> id -> parse_source_id(id)
        "ingest:collection:" <> id -> parse_source_id(id)
        _ -> []
      end)
      |> Enum.uniq()

    if source_ids == [], do: :unauthorized, else: {:ingest, source_ids}
  end

  defp parse_source_id(id) do
    case Integer.parse(id) do
      {source_id, ""} when source_id > 0 and source_id <= @max_source_id -> [source_id]
      _ -> []
    end
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
    with {:ok, source} <- Sources.fetch_source_by(token: token, user_id: user.id) do
      source = Sources.preload_defaults(source)
      json(conn, source)
    end
  end

  operation(:create,
    summary: "Create source",
    request_body: {"Source Parameters", "application/json", SourceParams.schema()},
    responses: %{
      201 => Created.response(Source),
      404 => NotFound.response(),
      422 => UnprocessableEntity.response()
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
    request_body: {"Source Parameters", "application/json", SourceParams.schema()},
    responses: %{
      204 => Accepted.response(),
      200 => Source.response(),
      404 => NotFound.response(),
      422 => UnprocessableEntity.response()
    }
  )

  def update(%{assigns: %{user: user}} = conn, %{"token" => token} = params) do
    with {:ok, source} <- Sources.fetch_source_by(token: token, user_id: user.id),
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
    with {:ok, source} <- Sources.fetch_source_by(token: token, user_id: user.id),
         {:ok, _} <- Sources.delete_source(source) do
      conn
      |> Plug.Conn.send_resp(204, [])
      |> Plug.Conn.halt()
    end
  end

  operation(:recent,
    summary: "Recent events in a source",
    parameters: [source_token: [in: :path, description: "Source Token", type: :string]],
    responses: %{
      200 => List.response(Event),
      404 => NotFound.response()
    }
  )

  def recent(%{assigns: %{user: user}} = conn, %{"source_token" => token}) do
    with {:ok, source} <- Sources.fetch_source_by(token: token, user_id: user.id) do
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

  def add_backend(%{assigns: %{user: user}} = conn, %{
        "source_token" => token,
        "backend_token" => backend_token
      }) do
    with {:ok, backend} <- Backends.fetch_backend_by(token: backend_token, user_id: user.id),
         {:ok, source} <- Sources.fetch_source_by(token: token, user_id: user.id),
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

  def remove_backend(%{assigns: %{user: user}} = conn, %{
        "source_token" => token,
        "backend_token" => backend_token
      }) do
    with {:ok, source} <- Sources.fetch_source_by(token: token, user_id: user.id),
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
    parameters: [source_token: [in: :path, description: "Source Token", type: :string]],
    responses: %{
      200 => OpenApiSchemas.SourceSchema.response(),
      404 => NotFound.response()
    }
  )

  def show_schema(%{assigns: %{user: user}} = conn, %{"source_token" => token} = params) do
    with {:ok, source} <- Sources.fetch_source_by(token: token, user_id: user.id) do
      schema = SourceSchemas.Cache.get_source_schema_by(source_id: source.id)

      data =
        if Map.get(params, "variant") == "dot" do
          SourceSchemas.format_schema(schema, :dot)
        else
          SourceSchemas.format_schema(schema, :json_schema, %{
            :title => source.name,
            :"$id" => ~p"/api/sources/#{source.token}/schema"
          })
        end

      json(conn, data)
    end
  end
end
