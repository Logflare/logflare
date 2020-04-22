defmodule LogflareWeb.LogController do
  use LogflareWeb, :controller

  plug CORSPlug,
       [
         origin: "*",
         max_age: 1_728_000,
         headers: [
           "Authorization",
           "Content-Type",
           "Content-Length",
           "X-Requested-With"
         ],
         methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
         send_preflight_response?: true
       ]
       when action in [:browser_reports]

  alias Logflare.Logs

  @message "Logged!"

  def create(%{assigns: %{source: source}} = conn, %{"batch" => batch}) when is_list(batch) do
    ingest_and_render(conn, batch, source)
  end

  def create(%{assigns: %{source: source}} = conn, log_params) do
    batch =
      log_params
      |> Map.take(~w[log_entry metadata timestamp])
      |> List.wrap()

    ingest_and_render(conn, batch, source)
  end

  def generic_json(%{assigns: %{source: source}} = conn, %{"_json" => batch})
      when is_list(batch) do
    batch =
      batch
      |> Logs.GenericJson.handle_batch()

    ingest_and_render(conn, batch, source)
  end

  def generic_json(%{assigns: %{source: source}, body_params: event} = conn, _log_params) do
    batch =
      event
      |> List.wrap()
      |> Logs.GenericJson.handle_batch()

    ingest_and_render(conn, batch, source)
  end

  def browser_reports(%{assigns: %{source: source}} = conn, %{"_json" => batch})
      when is_list(batch) do
    batch =
      batch
      |> Logs.BrowserReport.handle_batch()

    ingest_and_render(conn, batch, source)
  end

  def browser_reports(%{assigns: %{source: source}, body_params: event} = conn, _log_params) do
    batch =
      event
      |> List.wrap()
      |> Logs.BrowserReport.handle_batch()

    ingest_and_render(conn, batch, source)
  end

  def elixir_logger(%{assigns: %{source: source}} = conn, %{"batch" => batch})
      when is_list(batch) do
    ingest_and_render(conn, batch, source)
  end

  def vercel_ingest(%{assigns: %{source: source}} = conn, %{"_json" => batch})
      when is_list(batch) do
    batch =
      batch
      |> Logs.Vercel.handle_batch(source)

    ingest_and_render(conn, batch, source)
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
