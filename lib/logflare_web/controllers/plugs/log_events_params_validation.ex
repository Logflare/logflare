defmodule LogflareWeb.Plugs.LogEventParamsValidation do
  import Plug.Conn

  alias Logflare.{Logs}

  def init(_opts), do: nil

  def call(conn, _opts) do
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
