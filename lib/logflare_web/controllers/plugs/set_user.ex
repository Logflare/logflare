defmodule LogflareWeb.Plugs.SetUser do
  import Plug.Conn
  alias Logflare.{Users, User}

  def init(_), do: nil

  def call(%{assigns: %{user: %User{}}} = conn, _opts), do: conn

  def call(conn, _params) do
    user =
      conn
      |> get_session(:user_id)
      |> maybe_parse_binary_to_int()
      |> case do
        {int, ""} -> Users.Cache.get_by_id(int)
        _ -> nil
      end

    assign(conn, :user, user)
  end

  defp maybe_parse_binary_to_int(int) when is_integer(int), do: int

  defp maybe_parse_binary_to_int(session_user_id) do
    session_user_id && Integer.parse(session_user_id)
  end
end
