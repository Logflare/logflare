defmodule LogflareWeb.Plugs.SetUser do
  import Plug.Conn
  alias Logflare.{Users, User}

  def init(_params) do
  end

  def call(%{assigns: %{user: %User{}}} = conn, _params), do: conn

  def call(conn, _params) do
    user_id = get_session(conn, :user_id)

    cond do
      user = user_id && Users.Cache.get_by_id(user_id) ->
        assign(conn, :user, user)

      true ->
        assign(conn, :user, nil)
    end
  end
end
