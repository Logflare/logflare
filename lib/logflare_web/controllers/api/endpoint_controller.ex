defmodule LogflareWeb.Api.EndpointController do
  use LogflareWeb, :controller
  alias Logflare.Users
  alias Logflare.Endpoints
  action_fallback LogflareWeb.Api.FallbackController

  def index(%{assigns: %{user: user}} = conn, _) do
    user = Users.preload_endpoints(user)
    json(conn, user.endpoint_queries)
  end

  def show(%{assigns: %{user: user}} = conn, %{"token" => token}) do
    with query when not is_nil(query) <- Endpoints.get_by(token: token, user_id: user.id) do
      json(conn, query)
    end
  end

  def create(%{assigns: %{user: user}} = conn, params) do
    with {:ok, query} <- Endpoints.create_query(user, params) do
      conn
      |> put_status(201)
      |> json(query)
    end
  end

  def update(%{assigns: %{user: user}} = conn, %{"token" => token} = params) do
    with query when not is_nil(query) <- Endpoints.get_by(token: token, user_id: user.id),
         {:ok, query} <- Endpoints.update_query(query, params) do
      conn
      |> put_status(204)
      |> json(query)
    end
  end

  def delete(%{assigns: %{user: user}} = conn, %{"token" => token}) do
    with query when not is_nil(query) <- Endpoints.get_by(token: token, user_id: user.id),
         {:ok, _} <- Endpoints.delete_query(query) do
      conn
      |> Plug.Conn.send_resp(204, [])
      |> Plug.Conn.halt()
    end
  end
end
