defmodule LogtailWeb.Plugs.VerifySourceToken do
  import Plug.Conn
  import Phoenix.Controller

  alias Logtail.Repo
  alias Logtail.Source

  def init(_params) do

  end

  def call(conn, _params) do
    token = conn.params["source"]

    case Repo.get_by(Source, token: token) do
      nil ->
        conn
        |> put_status(403)
        |> redirect(to: "/api/error")
        |> halt()
      _ ->
        conn
    end
  end

end
