defmodule LogflareWeb.Plugs.CheckSourceCountApi do
  import Plug.Conn
  import Phoenix.Controller

  alias Logflare.AccountCache

  def init(_params) do
  end

  def call(conn, _params) do
    headers = Enum.into(conn.req_headers, %{})
    api_key = headers["x-api-key"]
    sources_count = AccountCache.count_sources(api_key)

    if sources_count < 101 do
      conn
    else
      message = "You have 100 sources. Delete one first!"

      conn
      |> put_status(403)
      |> put_view(LogflareWeb.LogView)
      |> render("index.json", message: message)
      |> halt()
    end
  end
end
