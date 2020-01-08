defmodule LogflareWeb.Plugs.CheckTeamUser do
  @moduledoc """
  Checks for team_user in the assigns, rejects if exists.
  """
  use Plug.Builder

  import Plug.Conn
  import Phoenix.Controller

  alias LogflareWeb.Router.Helpers, as: Routes

  def call(%{assigns: %{user: _user, team_user: _team_user}} = conn, _params), do: conn

  def call(%{assigns: %{user: _user}} = conn, _params) do
    conn
    |> put_flash(
      :info,
      "This is your account! You've been redirected to your account preferences."
    )
    |> redirect(to: Routes.user_path(conn, :edit))
    |> halt()
  end

  def call(conn, _params) do
    conn
  end
end
