defmodule LogflareWeb.Plugs.LogEventParamsValidation do
  import Plug.Conn

  alias Logflare.{Logs}

  def init(_params) do
  end

  def call(conn, _opts) do
    case Logs.validate_batch_params(conn.assigns.log_events) do
      :ok ->
        conn

      {:invalid, reason} ->
        Logs.RejectedEvents.injest(%{
          reason: reason,
          log_events: conn.assigns.log_events,
          source: conn.assigns.source
        })

        conn
        |> send_resp(406, reason)
        |> halt()
    end
  end
end
