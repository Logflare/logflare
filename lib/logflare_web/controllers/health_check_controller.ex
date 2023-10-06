defmodule LogflareWeb.HealthCheckController do
  use LogflareWeb, :controller

  alias Logflare.JSON
  alias Logflare.Cluster
  alias Logflare.SingleTenant

  def check(conn, _params) do
    {status, code} =
      if SingleTenant.supabase_mode?() do
        status = SingleTenant.supabase_mode_status()
        values = Map.values(status)

        if Enum.any?(values, &is_nil/1) do
          {:coming_up, 503}
        else
          {:ok, 200}
        end
      else
        {:ok, 200}
      end

    response =
      status
      |> build_payload()
      |> JSON.encode!()

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(code, response)
  end

  defp build_payload(status) when status in [:ok, :coming_up] do
    nodes = Cluster.Utils.node_list_all()
    proc_count = Process.list() |> Enum.count()

    %{
      status: status,
      proc_count: proc_count,
      this_node: Node.self(),
      nodes: nodes,
      nodes_count: Enum.count(nodes)
    }
  end
end
