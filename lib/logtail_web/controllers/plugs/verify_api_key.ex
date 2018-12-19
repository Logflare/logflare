defmodule LogtailWeb.Plugs.VerifyApiKey do
  import Plug.Conn
  import Phoenix.Controller

  alias Logtail.Repo
  alias Logtail.User

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
        |> put_view(LogtailWeb.LogView)
        |> render("index.json", message: message)
        |> halt()
      _ ->
        conn
    end
  end

end
