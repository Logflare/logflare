defmodule LogflareWeb.LogController do
  use LogflareWeb, :controller
  alias Logflare.Logs
  @message "Logged!"

  def create(conn, %{"log_entry" => _} = params) do
    params
    |> Map.take(~w[log_entry metadata timestamp])
    |> List.wrap()
    |> Logs.insert_logs(conn.assigns.source)

    render(conn, "index.json", message: @message)
  end

  def elixir_logger(conn, %{"batch" => batch}) do
    Logs.insert_logs(batch, conn.assigns.source)

    render(conn, "index.json", message: @message)
  end
end
