defmodule LogflareWeb.Api.IngestSourceController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.Sources
  alias LogflareWeb.Api.FallbackController
  alias LogflareWeb.OpenApi.List
  alias LogflareWeb.OpenApi.Unauthorized
  alias LogflareWeb.OpenApiSchemas.IngestSource
  alias OpenApiSpex.MediaType
  alias OpenApiSpex.Response
  alias OpenApiSpex.Schema

  action_fallback(FallbackController)

  tags(["ingest"])

  operation(:index,
    summary: "List sources available for ingestion",
    responses: %{
      200 => %Response{
        description: "Available ingest sources",
        content: %{
          "application/json" => %MediaType{schema: List.schema(IngestSource)},
          "text/csv" => %MediaType{schema: %Schema{type: :string}}
        }
      },
      401 => Unauthorized.response()
    }
  )

  def index(%{assigns: %{user: user}} = conn, _params) do
    access_token = Map.get(conn.assigns, :access_token)

    with {:ok, source_ids} <- authorized_source_ids(access_token) do
      sources = Sources.list_ingest_sources_by_user(user.id, source_ids)

      conn = put_resp_header(conn, "cache-control", "no-store")

      case get_format(conn) do
        "json" -> json(conn, sources)
        "csv" -> render(conn, :index, sources: sources)
      end
    end
  end

  defp authorized_source_ids(nil), do: {:ok, :all}

  defp authorized_source_ids(%{scopes: scopes}) do
    scopes = String.split(scopes || "")

    if scopes == [] or "public" in scopes or "private" in scopes or "ingest" in scopes do
      {:ok, :all}
    else
      source_ids =
        for scope <- scopes,
            [_, id] <- [Regex.run(~r/^ingest:(?:source|collection):(\d+)$/, scope)],
            do: String.to_integer(id)

      if source_ids == [], do: {:error, :unauthorized}, else: {:ok, source_ids}
    end
  end
end
