defmodule LogflareWeb.Plugs.LogEventParamsValidation do
  import Plug.Conn

  alias Logflare.{Logs}

  def init(_params) do
  end

  def call(conn, _opts) do
    case Logs.validate_log_entries(conn.assigns.raw_logs) do
      :ok ->
        conn

      {:invalid, reason} ->
        Logs.Rejected.injest(%{
          reason: reason,
          raw_logs: conn.assigns.raw_logs,
          source: conn.assigns.source
        })

        conn
        |> send_resp(406, reason)
        |> halt()
    end
  end
end
