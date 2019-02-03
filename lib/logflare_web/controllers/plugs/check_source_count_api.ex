defmodule LogflareWeb.Plugs.CheckSourceCountApi do
  import Plug.Conn
  import Phoenix.Controller
  import Ecto.Query, only: [from: 2]

  alias LogflareWeb.Router.Helpers
  alias Logflare.Repo
  alias Logflare.User


  def init(_params) do

  end

  def call(conn, _params) do
    headers = Enum.into(conn.req_headers, %{})
    api_key = headers["x-api-key"]
    user_id = Repo.get_by(User, api_key: api_key).id

    query = from s in "sources",
          where: s.user_id == ^user_id,
          select: count(s.id)

    sources_count = Repo.one(query)

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
