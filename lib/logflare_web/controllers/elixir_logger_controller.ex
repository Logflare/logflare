defmodule LogflareWeb.ElixirLoggerController do
  use LogflareWeb, :controller
  alias Logflare.Logs

  def create(conn, %{"batch" => batch} = params) do
    message = "Logged!"

    with {:ok, _message} <- Logs.insert_all(batch, conn.assigns.source) do
      render(conn, "index.json", message: message)
    else
      {:error, "message"} ->
        send_resp(conn, 406, "Nested values must be of the same type")
    end
  end
end
