defmodule LogflareWeb.Api.AlertIssueTypeController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.AlertIssues.IssueTypes
  alias LogflareWeb.Api.AlertIssueFilters
  alias LogflareWeb.OpenApi.BadRequest
  alias LogflareWeb.OpenApi.List
  alias LogflareWeb.OpenApiSchemas.AlertIssueTypeApiSchema

  action_fallback(LogflareWeb.Api.FallbackController)

  tags(["management"])

  @filters_param [
    filters: [
      in: :query,
      description:
        "Identifier filters that silences must equal, e.g. filters[identifiers.project]=abcd1234",
      type: :object,
      style: :deepObject,
      required: true
    ]
  ]

  operation(:index,
    summary: "List alert issue types",
    parameters: @filters_param,
    responses: %{200 => List.response(AlertIssueTypeApiSchema), 400 => BadRequest.response()}
  )

  def index(%{assigns: %{user: user}} = conn, params) do
    case AlertIssueFilters.parse(Map.get(params, "filters")) do
      {:ok, identifiers} -> json(conn, IssueTypes.list(user, identifiers))
      {:error, message} -> conn |> put_status(400) |> json(%{error: message})
    end
  end
end
