defmodule LogflareWeb.Plugs.CheckSourceCountApi do
  import Plug.Conn
  import Phoenix.Controller

  alias Logflare.Users

  def init(_params) do
  end

  def call(conn, _params) do
    headers = Enum.into(conn.req_headers, %{})
    api_key = headers["x-api-key"]
    user = Users.Cache.find_user_by_api_key(api_key)

    if length(user.sources) < 101 do
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
