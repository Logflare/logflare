defmodule LogflareWeb.Plugs.SetApiUser do
  import Plug.Conn

  alias Logflare.Repo
  alias Logflare.User

  def init(_params) do
  end

  def call(conn, _params) do
    headers = Enum.into(conn.req_headers, %{})
    api_key = headers["x-api-key"]

    cond do
      user = api_key && Repo.get_by(User, api_key: api_key) ->
        assign(conn, :user, user)

      true ->
        assign(conn, :user, nil)
    end
  end
end
