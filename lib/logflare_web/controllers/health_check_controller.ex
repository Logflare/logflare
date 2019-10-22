defmodule LogflareWeb.HealthCheckController do
  use LogflareWeb, :controller

  alias Logflare.Cluster

  def check(conn, params) do
    nodes = Cluster.Utils.node_list_all()

    response =
      %{status: :ok, nodes: nodes, nodes_count: Enum.count(nodes)}
      |> Jason.encode!()

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, response)
  end
end
