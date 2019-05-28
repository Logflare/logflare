defmodule LogflareWeb.LogController do
  use LogflareWeb, :controller
  alias Logflare.Logs

  def create(conn, %{"log_entry" => _} = params) do
    message = "Logged!"

    params
    |> Map.take(~w[log_entry metadata timestamp])
    |> List.wrap()
    |> Logs.insert_logs(conn.assigns.source)

    render(conn, "index.json", message: message)
  end
end
