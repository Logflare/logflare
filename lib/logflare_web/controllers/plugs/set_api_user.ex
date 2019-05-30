defmodule LogflareWeb.Plugs.SetApiUser do
  import Plug.Conn
  alias Logflare.Users

  def init(_opts), do: nil

  def call(conn, _opts) do
    user =
      conn.req_headers
      |> Enum.into(%{})
      |> Map.get("x-api-key")
      |> Users.Cache.find_user_by_api_key()

    assign(conn, :user, user)
  end
end
