defmodule LogflareWeb.Plugs.VerifyApiRequest do
  use Plug.Builder
  import Phoenix.Controller
  alias Logflare.{Users, User, Source, Sources, Logs}

  require Logger

  plug :check_log_entry
  plug :validate_log_events

  def check_log_entry(%{params: params} = conn, _opts \\ []) do
    log_entry = params["log_entry"] || params["batch"]

    if log_entry in [%{}, [], "", nil] do
      message = "Log entry needed."

      conn
      |> put_status(406)
      |> put_view(LogflareWeb.LogView)
      |> render("index.json", message: message)
      |> halt()
    else
      raw_logs =
        params
        |> Map.get("batch", params)
        |> List.wrap()

      conn
      |> assign(:log_events, raw_logs)
    end
  end

  def validate_log_events(conn, _opts \\ []) do
    case Logs.validate_batch_params(conn.assigns.log_events) do
      :ok ->
        conn

      {:invalid, validator} ->
        Logs.RejectedEvents.injest(%{
          error: validator,
          batch: conn.assigns.log_events,
          source: conn.assigns.source
        })

        conn
        |> send_resp(406, validator.message)
        |> halt()
    end
  end
end
