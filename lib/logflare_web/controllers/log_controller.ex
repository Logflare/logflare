defmodule LogflareWeb.LogController do
  use LogflareWeb, :controller
  alias Logflare.Logs
  @message "Logged!"

  def create(conn, %{"batch" => batch}) when is_list(batch) do
    ingest_and_render(conn, batch, conn.assigns.source)
  end

  def create(conn, log_params) do
    batch =
      log_params
      |> Map.take(~w[log_entry metadata timestamp])
      |> List.wrap()

    ingest_and_render(conn, batch, conn.assigns.source)
  end

  def elixir_logger(conn, %{"batch" => batch}) when is_list(batch) do
    batch =
      for log_params <- batch do
        Map.update(
          log_params,
          "metadata",
          %{},
          &Map.drop(&1, ["domain", "gl", "mfa", "time", "report_cb", "error"])
        )
      end

    ingest_and_render(conn, batch, conn.assigns.source)
  end

  def zeit_ingest(conn, %{"_json" => batch}) do
    batch = Logs.Zeit.handle_batch(batch, conn.assigns.source)

    ingest_and_render(conn, batch, conn.assigns.source)
  end

  def ingest_and_render(conn, log_params_batch, source) do
    case Logs.ingest_logs(log_params_batch, source) do
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
