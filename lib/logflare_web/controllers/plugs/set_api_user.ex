defmodule LogflareWeb.Plugs.SetApiUser do
  import Plug.Conn
  alias Logflare.{Users, User}

  def init(_params) do
  end

  def call(conn, _params) do
    headers = Enum.into(conn.req_headers, %{})
    api_key = headers["x-api-key"]

    user = Users.Cache.find_user_by_api_key(api_key)

    case user do
      %User{api_key: api_key} ->
        assign(conn, :user, api_key)

      _ ->
        assign(conn, :user, nil)
    end
  end
end
