defmodule LogflareWeb.Plugs.SetHeaders do
  import Plug.Conn

  alias Logflare.User
  alias Logflare.TeamUsers.TeamUser

  def init(opts), do: opts

  def call(%{assigns: %{user: %User{} = user, team_user: %TeamUser{} = team_user}} = conn, _opts) do
    conn
    |> put_resp_header("logflare-node", Atom.to_string(Node.self()))
    |> put_resp_header("logflare-user-id", Integer.to_string(user.id))
    |> put_resp_header("logflare-team-user-id", Integer.to_string(team_user.id))
  end

  def call(%{assigns: %{user: %User{} = user}} = conn, _opts) do
    conn
    |> put_resp_header("logflare-node", Atom.to_string(Node.self()))
    |> put_resp_header("logflare-user-id", Integer.to_string(user.id))
  end

  def call(conn, _opts) do
    put_resp_header(conn, "logflare-node", Atom.to_string(Node.self()))
  end
end
