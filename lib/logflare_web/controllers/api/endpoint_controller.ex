defmodule LogflareWeb.Api.EndpointController do
  use LogflareWeb, :controller
  alias Logflare.Users

  action_fallback LogflareWeb.Api.FallbackController

  def index(%{assigns: %{user: user}} = conn, _) do
    user = Users.preload_endpoints(user)
    json(conn, user.endpoint_queries)
  end
end
