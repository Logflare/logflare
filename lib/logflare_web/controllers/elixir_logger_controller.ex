defmodule LogflareWeb.ElixirLoggerController do
  use LogflareWeb, :controller
  alias Logflare.Logs

  def create(conn, %{"batch" => batch}) do
    message = "Logged!"

    result = Logs.insert_logs(batch, conn.assigns.source)

    render(conn, "index.json", message: message)
  end
end
