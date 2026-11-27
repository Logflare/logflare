defmodule LogflareWeb.Api.SourceController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.Backends
  alias Logflare.EnrichmentRules
  alias Logflare.Sources
  alias Logflare.SourceSchemas
  alias LogflareWeb.OpenApi.Accepted
  alias LogflareWeb.OpenApi.Created
  alias LogflareWeb.OpenApi.List
  alias LogflareWeb.OpenApi.NotFound
  alias LogflareWeb.OpenApi.UnprocessableEntity
  alias LogflareWeb.OpenApiSchemas.Event

  alias LogflareWeb.OpenApiSchemas.Source
  alias LogflareWeb.OpenApiSchemas.SourceParams
  alias LogflareWeb.OpenApiSchemas

  action_fallback(LogflareWeb.Api.FallbackController)

  tags(["management"])

  operation(:index,
    summary: "List sources",
    responses: %{200 => List.response(Source)}
  )

  def index(%{assigns: %{user: user}} = conn, _) do
    sources = Sources.list_sources_by_user(user.id) |> Sources.preload_for_dashboard()
    json(conn, with_enrichment_rules(sources))
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
      json(conn, with_enrichment_rules(source))
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
    with :ok <- validate_enrichment_rules(params),
         {:ok, source} <- Sources.create_source(params, user),
         {:ok, _rules} <- replace_enrichment_rules(source, params) do
      source = Sources.preload_defaults(source)

      conn
      |> put_status(201)
      |> json(with_enrichment_rules(source))
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
         :ok <- validate_enrichment_rules(params),
         {:ok, source} <- Sources.update_source_by_user(source, params),
         {:ok, _rules} <- replace_enrichment_rules(source, params) do
      source = Sources.preload_defaults(source)

      conn
      |> case do
        %{method: "PATCH"} ->
          conn |> send_resp(204, "")

        %{method: "PUT"} ->
          conn
          |> put_status(200)
          |> json(with_enrichment_rules(source))
      end
    end
  end

  defp validate_enrichment_rules(%{"enrichment_rules" => rules}),
    do: EnrichmentRules.validate_params(rules)

  defp validate_enrichment_rules(_params), do: :ok

  defp replace_enrichment_rules(source, %{"enrichment_rules" => rules}),
    do: EnrichmentRules.replace_for_source(source, rules)

  defp replace_enrichment_rules(_source, _params), do: {:ok, :unchanged}

  defp with_enrichment_rules(sources) when is_list(sources) do
    rules = sources |> Enum.map(& &1.id) |> EnrichmentRules.list_by_source_ids()
    Enum.map(sources, &put_enrichment_rules(&1, Map.get(rules, &1.id, [])))
  end

  defp with_enrichment_rules(source),
    do: put_enrichment_rules(source, EnrichmentRules.list_by_source_id(source.id))

  defp put_enrichment_rules(source, rules) do
    source
    |> Jason.encode!()
    |> Jason.decode!()
    |> Map.put("enrichment_rules", Enum.map(rules, &EnrichmentRules.to_api/1))
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
