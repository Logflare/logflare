defmodule LogflareWeb.Plugs.VerifySourceOwner do
  use Plug.Builder

  import Plug.Conn
  import Phoenix.Controller
  alias Logflare.Users
  alias LogflareWeb.Router.Helpers, as: Routes

  def call(%{assigns: %{user: %{admin: true}}} = conn, _opts), do: conn

  def call(%{assigns: %{user: user}, params: params} = conn, _opts) do
    pk = params["id"] || params["source_id"]

    if Users.find_source_by_pk(user, pk) do
      conn
    else
      conn
      |> put_status(401)
      |> put_flash(:error, "That's not yours!")
      |> redirect(to: Routes.marketing_path(conn, :index))
      |> halt()
    end
  end
end
