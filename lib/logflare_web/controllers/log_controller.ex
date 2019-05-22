defmodule LogflareWeb.LogController do
  use LogflareWeb, :controller
  alias Logflare.Logs

  def create(conn, %{"log_entry" => _} = params) do
    message = "Logged!"

    result =
      params
      |> Map.take(~w[log_entry metadata timestamp])
      |> List.wrap()
      |> Logs.insert_logs(conn.assigns.source)

    with :ok <- result do
      render(conn, "index.json", message: message)
    else
      {:error, message} ->
        send_resp(conn, 406, message)
    end
  end
end
