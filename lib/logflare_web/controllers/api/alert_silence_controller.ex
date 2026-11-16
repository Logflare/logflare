defmodule LogflareWeb.Api.AlertSilenceController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.AlertIssues.Silences
  alias LogflareWeb.Api.AlertIssueFilters
  alias LogflareWeb.OpenApi.BadRequest
  alias LogflareWeb.OpenApi.Created
  alias LogflareWeb.OpenApi.List
  alias LogflareWeb.OpenApi.NotFound
  alias LogflareWeb.OpenApi.UnprocessableEntity
  alias LogflareWeb.OpenApiSchemas.AlertIssueSilenceApiSchema

  action_fallback(LogflareWeb.Api.FallbackController)

  tags(["management"])

  operation(:index,
    summary: "List alert issue silences",
    parameters: [
      filters: [
        in: :query,
        description: "Identifiers the silences' matchers must be satisfied by",
        type: :object,
        style: :deepObject,
        required: true
      ]
    ],
    responses: %{200 => List.response(AlertIssueSilenceApiSchema), 400 => BadRequest.response()}
  )

  def index(%{assigns: %{user: user}} = conn, params) do
    case AlertIssueFilters.parse(Map.get(params, "filters")) do
      {:ok, identifiers} -> json(conn, Silences.list_matching(user, identifiers))
      {:error, message} -> conn |> put_status(400) |> json(%{error: message})
    end
  end

  operation(:create,
    summary: "Create an alert issue silence",
    request_body:
      {"Silence", "application/json",
       %OpenApiSpex.Schema{
         type: :object,
         required: [:matchers],
         properties: %{
           rule_key: %OpenApiSpex.Schema{type: :string, nullable: true},
           matchers: %OpenApiSpex.Schema{
             type: :object,
             additionalProperties: %OpenApiSpex.Schema{type: :string}
           },
           comment: %OpenApiSpex.Schema{type: :string},
           created_by: %OpenApiSpex.Schema{type: :string}
         }
       }},
    responses: %{
      201 => Created.response(AlertIssueSilenceApiSchema),
      422 => UnprocessableEntity.response()
    }
  )

  def create(%{assigns: %{user: user}} = conn, params) do
    attrs =
      params
      |> Map.take(["rule_key", "matchers", "comment", "created_by"])
      |> Map.put_new("created_by", user.email)

    with {:ok, silence} <- Silences.create(user, attrs) do
      conn |> put_status(201) |> json(silence)
    end
  end

  operation(:delete,
    summary: "Delete an alert issue silence",
    parameters: [id: [in: :path, description: "Silence ID", type: :integer]],
    responses: %{204 => "No Content", 404 => NotFound.response()}
  )

  def delete(%{assigns: %{user: user}} = conn, %{"id" => id}) do
    with :ok <- Silences.delete(user, id) do
      send_resp(conn, 204, "")
    end
  end
end
