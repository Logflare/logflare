defmodule LogflareWeb.EndpointsController do
  use LogflareWeb, :controller
  require Logger
  alias Logflare.Endpoints
  alias Logflare.User

  action_fallback(LogflareWeb.Api.FallbackController)

  plug CORSPlug,
    origin: "*",
    max_age: 1_728_000,
    headers: [
      "Authorization",
      "Content-Type",
      "Content-Length",
      "X-Requested-With",
      "X-API-Key"
    ],
    methods: ["GET", "POST", "OPTIONS"],
    send_preflight_response?: true

  def query(%{assigns: assigns} = conn, %{"token" => token}) do
    endpoint_query = Endpoints.get_mapped_query_by_token(token)
    user = Map.get(assigns, :user)

    with :ok <- check_auth(endpoint_query, user),
         {:ok, result} <- Endpoints.run_cached_query(endpoint_query, conn.query_params) do
      Logger.debug("Endpoint cache result, #{inspect(result, pretty: true)}")
      render(conn, "query.json", result: result.rows)
    else
      {:error, :unauthorized} = err ->
        err

      {:error, err} ->
        render(conn, "query.json", error: err)
    end
  end

  defp check_auth(nil, _), do: {:error, :not_found}
  defp check_auth(%{enable_auth: false}, _), do: :ok
  defp check_auth(_endpoint, nil), do: :ok

  defp check_auth(endpoint, %User{id: id}) do
    if endpoint.user_id == id do
      :ok
    else
      {:error, :unauthorized}
    end
  end
end
