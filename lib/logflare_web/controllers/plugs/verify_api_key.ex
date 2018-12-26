defmodule LogflareWeb.Plugs.VerifyApiKey do
  import Plug.Conn
  import Phoenix.Controller

  alias Logflare.Repo
  alias Logflare.User

  def init(_params) do

  end

  def call(conn, _params) do
    headers = Enum.into(conn.req_headers, %{})
    api_key = headers["x-api-key"]

    case Repo.get_by(User, api_key: api_key) do
      nil ->
        message = "Unknown x-api-key."
        conn
        |> put_status(403)
        |> put_view(LogflareWeb.LogView)
        |> render("index.json", message: message)
        |> halt()
      _ ->
        conn
    end
  end

#  def call(conn, _params) do
#    source = conn.params["source"]
#    source_name = conn.params["source_name"]
#    both_nil = source <> source_name
#
#    case source AND source_name == nil do
#      true ->
#        message = "Source or source_name needed."
#        conn
#        |> put_status(403)
#        |> put_view(LogflareWeb.LogView)
#        |> render("index.json", message: message)
#        |> halt()
#      false ->
#        conn
#    end
#  end

end
