defmodule LogflareWeb.Plugs.VerifyApiRequest do
  use Plug.Builder
  import Phoenix.Controller
  alias Logflare.{Users, User, Source}

  require Logger

  alias Logflare.Sources

  plug :check_log_entry

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
end
