defmodule LogflareWeb.LogController do
  use LogflareWeb, :controller
  alias Logflare.Logs
  @message "Logged!"

  def create(conn, %{"log_entry" => log_entry} = params) when is_binary(log_entry) do
    batch =
      params
      |> Map.take(~w[log_entry metadata timestamp])
      |> List.wrap()

    injest_and_render(conn, batch, conn.assigns.source)
  end

  def elixir_logger(conn, %{"batch" => batch}) when is_list(batch) do
    injest_and_render(conn, batch, conn.assigns.source)
  end

  def injest_and_render(conn, log_params_batch, source) do
    case Logs.injest_logs(log_params_batch, source) do
      :ok ->
        render(conn, "index.json", message: @message)

      {:error, errors} ->
        conn
        |> put_status(406)
        |> put_view(LogflareWeb.LogView)
        |> render("index.json", message: errors)
    end
  end
end
