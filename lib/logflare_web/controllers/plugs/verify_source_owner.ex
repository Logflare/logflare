defmodule LogflareWeb.Plugs.VerifySourceOwner do
  use Plug.Builder

  import Plug.Conn
  import Phoenix.Controller
  alias Logflare.{Users, Sources}
  alias LogflareWeb.Router.Helpers, as: Routes

  def call(%{assigns: %{user: %{admin: true}}} = conn, _opts), do: conn

  def call(%{assigns: %{user: user}, params: params} = conn, _opts) do
    pk = params["id"] || params["source_id"]

    if Sources.get_by(id: pk).user_id == user.id do
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
