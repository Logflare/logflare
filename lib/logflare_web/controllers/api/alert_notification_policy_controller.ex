defmodule LogflareWeb.Api.AlertNotificationPolicyController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.AlertIssues.NotificationPolicies
  alias LogflareWeb.OpenApi.Created
  alias LogflareWeb.OpenApi.List
  alias LogflareWeb.OpenApi.NotFound
  alias LogflareWeb.OpenApi.One
  alias LogflareWeb.OpenApi.UnprocessableEntity
  alias LogflareWeb.OpenApiSchemas.AlertNotificationPolicyApiParams
  alias LogflareWeb.OpenApiSchemas.AlertNotificationPolicyApiSchema

  action_fallback(LogflareWeb.Api.FallbackController)

  tags(["management"])

  @fields ~w(name enabled rule_keys min_severity categories identifiers events repeat_interval_seconds receivers)
  @id_param [id: [in: :path, description: "Notification policy ID", type: :integer]]

  operation(:index,
    summary: "List alert issue notification policies",
    responses: %{200 => List.response(AlertNotificationPolicyApiSchema)}
  )

  def index(%{assigns: %{user: user}} = conn, _params) do
    json(conn, NotificationPolicies.list(user))
  end

  operation(:show,
    summary: "Fetch an alert issue notification policy",
    parameters: @id_param,
    responses: %{
      200 => One.response(AlertNotificationPolicyApiSchema),
      404 => NotFound.response()
    }
  )

  def show(%{assigns: %{user: user}} = conn, %{"id" => id}) do
    with {:ok, policy} <- NotificationPolicies.get(user, id) do
      json(conn, policy)
    end
  end

  operation(:create,
    summary: "Create an alert issue notification policy",
    request_body: AlertNotificationPolicyApiParams.params(),
    responses: %{
      201 => Created.response(AlertNotificationPolicyApiSchema),
      422 => UnprocessableEntity.response()
    }
  )

  def create(%{assigns: %{user: user}} = conn, params) do
    with {:ok, policy} <- NotificationPolicies.create(user, Map.take(params, @fields)) do
      conn |> put_status(201) |> json(policy)
    end
  end

  operation(:update,
    summary: "Update an alert issue notification policy",
    parameters: @id_param,
    request_body: AlertNotificationPolicyApiParams.params(),
    responses: %{
      200 => One.response(AlertNotificationPolicyApiSchema),
      404 => NotFound.response(),
      422 => UnprocessableEntity.response()
    }
  )

  def update(%{assigns: %{user: user}} = conn, %{"id" => id} = params) do
    with {:ok, policy} <- NotificationPolicies.update(user, id, Map.take(params, @fields)) do
      json(conn, policy)
    end
  end

  operation(:delete,
    summary: "Delete an alert issue notification policy",
    parameters: @id_param,
    responses: %{204 => "No Content", 404 => NotFound.response()}
  )

  def delete(%{assigns: %{user: user}} = conn, %{"id" => id}) do
    with :ok <- NotificationPolicies.delete(user, id) do
      send_resp(conn, 204, "")
    end
  end
end
