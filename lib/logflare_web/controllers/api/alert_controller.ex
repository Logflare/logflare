defmodule LogflareWeb.Api.AlertController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.Alerting
  alias Logflare.Backends
  alias LogflareWeb.OpenApi.Accepted
  alias LogflareWeb.OpenApi.BadRequest
  alias LogflareWeb.OpenApi.Created
  alias LogflareWeb.OpenApi.List
  alias LogflareWeb.OpenApi.NotFound
  alias LogflareWeb.OpenApi.UnprocessableEntity
  alias LogflareWeb.OpenApiSchemas.AlertApiCreateParams
  alias LogflareWeb.OpenApiSchemas.AlertApiSchema
  alias LogflareWeb.OpenApiSchemas.AlertApiUpdateParams

  action_fallback(LogflareWeb.Api.FallbackController)

  tags(["management"])

  operation(:index,
    summary: "List alerts",
    responses: %{200 => List.response(AlertApiSchema)}
  )

  def index(%{assigns: %{user: user}} = conn, _params) do
    alerts =
      user
      |> Alerting.list_alert_queries_user_access()
      |> Enum.map(&Alerting.preload_alert_query/1)

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
      json(conn, Alerting.preload_alert_query(alert))
    end
  end

  operation(:create,
    summary: "Create alert",
    request_body: AlertApiCreateParams.params(),
    responses: %{
      201 => Created.response(AlertApiSchema),
      400 => BadRequest.response(),
      404 => NotFound.response(),
      422 => UnprocessableEntity.response()
    }
  )

  def create(%{assigns: %{user: user}} = conn, params) do
    with {:ok, attrs} <- verify_backends_owner(params, user),
         {:ok, alert} <- Alerting.create_alert_query(user, attrs) do
      conn
      |> put_status(201)
      |> json(Alerting.preload_alert_query(alert))
    end
  end

  operation(:update,
    summary: "Update alert",
    parameters: [token: [in: :path, description: "Alert UUID", type: :string]],
    request_body: AlertApiUpdateParams.params(),
    responses: %{
      204 => Accepted.response(),
      200 => Accepted.response(AlertApiSchema),
      400 => BadRequest.response(),
      404 => NotFound.response(),
      422 => UnprocessableEntity.response()
    }
  )

  def update(%{assigns: %{user: user}} = conn, %{"token" => token} = params) do
    with {:ok, alert} <- Alerting.fetch_alert_query_by_user_access(user, token: token),
         {:ok, attrs} <- verify_backends_owner(params, user),
         {:ok, updated} <- Alerting.update_alert_query(alert, attrs) do
      conn
      |> case do
        %{method: "PATCH"} ->
          conn
          |> put_status(204)
          |> text("")

        %{method: "PUT"} ->
          put_status(conn, 200)
          |> json(Alerting.preload_alert_query(updated))
      end
    end
  end

  defp verify_backends_owner(%{"backends" => _}, _user) do
    {:error, "backends is read-only; use backend_ids"}
  end

  defp verify_backends_owner(%{"backend_ids" => ids}, _user) when not is_list(ids) do
    {:error, "backend_ids must be an array"}
  end

  defp verify_backends_owner(%{"backend_ids" => ids} = params, user) do
    with :ok <- validate_backend_ids(ids),
         {:ok, backends} <- fetch_backends(Enum.uniq(ids), user) do
      attrs =
        params
        |> Map.delete("backend_ids")
        |> Map.put("backends", backends)

      {:ok, attrs}
    end
  end

  defp verify_backends_owner(params, _user), do: {:ok, params}

  defp validate_backend_ids(ids) do
    if Enum.all?(ids, &(is_integer(&1) and &1 > 0)),
      do: :ok,
      else: {:error, "backend_ids must contain positive integers"}
  end

  defp fetch_backends(ids, user), do: fetch_backends(ids, user, [])

  defp fetch_backends([], _user, acc), do: {:ok, Enum.reverse(acc)}

  defp fetch_backends([id | ids], user, acc) do
    case Backends.get_backend_by_user_access(user, id) do
      nil -> {:error, :not_found}
      backend -> fetch_backends(ids, user, [backend | acc])
    end
  end

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
      |> put_status(204)
      |> text("")
      |> halt()
    end
  end
end
