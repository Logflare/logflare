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
    with source when not is_nil(source) <- Endpoints.get_by(token: token, user_id: user.id) do
      json(conn, source)
    end
  end
end
