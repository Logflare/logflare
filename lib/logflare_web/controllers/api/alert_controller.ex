defmodule LogflareWeb.Api.AlertController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.Alerting
  alias Logflare.Backends
  alias LogflareWeb.OpenApi.Accepted
  alias LogflareWeb.OpenApi.Created
  alias LogflareWeb.OpenApi.List
  alias LogflareWeb.OpenApi.NotFound
  alias LogflareWeb.OpenApiSchemas.AlertApiSchema

  action_fallback(LogflareWeb.Api.FallbackController)

  tags(["management"])

  operation(:index,
    summary: "List alerts",
    responses: %{200 => List.response(AlertApiSchema)}
  )

  def index(%{assigns: %{user: user}} = conn, _params) do
    alerts = Alerting.list_alert_queries_user_access(user)
    json(conn, alerts)
  end

  operation(:show,
    summary: "Fetch alert",
    parameters: [token: [in: :path, description: "Alert UUID", type: :string]],
    responses: %{
      200 => AlertApiSchema.response(),
      404 => NotFound.response()
    }
  )

  def show(%{assigns: %{user: user}} = conn, %{"token" => token}) do
    with {:ok, alert} <- Alerting.fetch_alert_query_by_user_access(user, token: token) do
      json(conn, alert)
    end
  end

  operation(:create,
    summary: "Create alert",
    request_body: AlertApiSchema.params(),
    responses: %{
      201 => Created.response(AlertApiSchema),
      404 => NotFound.response()
    }
  )

  def create(%{assigns: %{user: user}} = conn, params) do
    with {:ok, alert} <- Alerting.create_alert_query(user, params) do
      conn
      |> put_status(201)
      |> json(alert)
    end
  end

  operation(:update,
    summary: "Update alert",
    parameters: [token: [in: :path, description: "Alert UUID", type: :string]],
    request_body: AlertApiSchema.params(),
    responses: %{
      204 => Accepted.response(),
      200 => Accepted.response(AlertApiSchema),
      404 => NotFound.response()
    }
  )

  def update(%{assigns: %{user: user}} = conn, %{"token" => token} = params) do
    with {:ok, alert} <- Alerting.fetch_alert_query_by_user_access(user, token: token),
         {:ok, attrs} <- verify_backends_owner(params, user),
         {:ok, updated} <- Alerting.update_alert_query(alert, attrs) do
      conn
      |> case do
        %{method: "PATCH"} ->
          send_resp(conn, 204, "")

        %{method: "PUT"} ->
          put_status(conn, 200)
          |> json(updated)
      end
    end
  end

  defp verify_backends_owner(%{"backend_ids" => ids} = params, user) do
    ids
    |> Enum.reduce_while({:ok, []}, fn id, {:ok, acc} ->
      case Backends.fetch_backend_by(id: id, user_id: user.id) do
        {:ok, backend} -> {:cont, {:ok, [backend | acc]}}
        {:error, :not_found} -> {:halt, {:error, :not_found}}
      end
    end)
    |> case do
      {:ok, backends} ->
        attrs =
          params
          |> Map.delete("backend_ids")
          |> Map.put("backends", Enum.reverse(backends))

        {:ok, attrs}

      error ->
        error
    end
  end

  defp verify_backends_owner(params, _user), do: {:ok, params}

  operation(:delete,
    summary: "Delete alert",
    parameters: [token: [in: :path, description: "Alert UUID", type: :string]],
    responses: %{
      204 => Accepted.response(),
      404 => NotFound.response()
    }
  )

  def delete(%{assigns: %{user: user}} = conn, %{"token" => token}) do
    with {:ok, alert} <- Alerting.fetch_alert_query_by_user_access(user, token: token),
         {:ok, _} <- Alerting.delete_alert_query(alert) do
      conn
      |> send_resp(204, [])
      |> halt()
    end
  end
end
