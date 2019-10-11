defmodule LogflareWeb.ClusterController do
  use LogflareWeb, :controller
  alias LogflareWeb.ClusterLV

  def index(conn, _params) do
    live_render(conn, ClusterLV, session: %{})
  end

end
