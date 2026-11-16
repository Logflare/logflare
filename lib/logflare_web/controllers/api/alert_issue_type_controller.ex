defmodule LogflareWeb.Api.AlertIssueTypeController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.AlertIssues.IssueTypes
  alias LogflareWeb.Api.AlertIssueFilters
  alias LogflareWeb.OpenApi.BadRequest
  alias LogflareWeb.OpenApi.Created
  alias LogflareWeb.OpenApi.List
  alias LogflareWeb.OpenApi.NotFound
  alias LogflareWeb.OpenApi.UnprocessableEntity
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

  operation(:create_custom,
    summary: "Create a custom LQL alert issue type",
    request_body:
      {"Custom alert issue type", "application/json",
       %OpenApiSpex.Schema{
         type: :object,
         required: [:name, :lql, :identifiers],
         properties: %{
           name: %OpenApiSpex.Schema{type: :string},
           description: %OpenApiSpex.Schema{type: :string},
           lql: %OpenApiSpex.Schema{
             type: :string,
             description: "Must name a source with f:<source>"
           },
           severity: %OpenApiSpex.Schema{
             type: :string,
             enum: ["critical", "major", "warning", "info"]
           },
           category: %OpenApiSpex.Schema{
             type: :string,
             enum: ["security", "performance", "health"]
           },
           group_by: %OpenApiSpex.Schema{type: :array, items: %OpenApiSpex.Schema{type: :string}},
           identifiers: %OpenApiSpex.Schema{
             type: :object,
             additionalProperties: %OpenApiSpex.Schema{type: :string}
           },
           notebook: %OpenApiSpex.Schema{
             type: :array,
             maxItems: 10,
             items: %OpenApiSpex.Schema{
               type: :object,
               required: [:title, :source, :sql],
               properties: %{
                 title: %OpenApiSpex.Schema{type: :string},
                 source: %OpenApiSpex.Schema{type: :string, enum: ["database", "logs"]},
                 sql: %OpenApiSpex.Schema{type: :string}
               }
             }
           }
         }
       }},
    responses: %{
      201 => Created.response(AlertIssueTypeApiSchema),
      422 => UnprocessableEntity.response()
    }
  )

  def create_custom(%{assigns: %{user: user}} = conn, params) do
    with {:ok, issue_type} <- IssueTypes.create_custom(user, params) do
      conn |> put_status(201) |> json(issue_type)
    end
  end

  operation(:delete_custom,
    summary: "Delete a custom alert issue type",
    parameters: [rule_key: [in: :path, description: "Rule key", type: :string]],
    responses: %{
      204 => "No Content",
      404 => NotFound.response(),
      422 => {"Managed alert issue type", "application/json", BadRequest.schema()}
    }
  )

  def delete_custom(%{assigns: %{user: user}} = conn, %{"rule_key" => rule_key}) do
    case IssueTypes.delete_custom(user, rule_key) do
      :ok ->
        send_resp(conn, 204, "")

      {:error, :managed} ->
        conn |> put_status(422) |> json(%{error: "managed alert issue types cannot be deleted"})

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end
end
