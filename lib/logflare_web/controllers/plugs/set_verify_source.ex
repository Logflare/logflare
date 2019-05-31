defmodule LogflareWeb.Plugs.SetVerifySource do
  use Plug.Builder

  import Plug.Conn
  import Phoenix.Controller
  alias Logflare.{Sources, Source}
  alias LogflareWeb.Router.Helpers, as: Routes

  def call(%{assigns: %{source: %Source{}}} = conn, _opts), do: conn

  def call(%{assigns: %{user: user}, params: params} = conn, _opts) do
    id = params["id"] || params["source_id"]
    token = params["source"] || params["source_id"]
    name = params["source_name"]

    is_api_path = conn.request_path =~ "/logs" or conn.request_path =~ "/api"
    is_browser_path = not is_api_path

    source =
      cond do
        token && is_api_path ->
          Sources.Cache.get_by(token: token)

        id && is_browser_path ->
          Sources.Cache.get_by(id: id)

        name ->
          Sources.Cache.get_by(name: name)

        true ->
          nil
      end

    user_authorized = source.user_id === user.id || user.admin

    case {source && user_authorized, is_api_path} do
      {true, _} ->
        assign(conn, :source, source)

      {false, true} ->
        message = "Source is not owned by this user."

        conn
        |> put_status(403)
        |> put_view(LogflareWeb.LogView)
        |> render("index.json", message: message)
        |> halt()

      {false, false} ->
        conn
        |> put_status(403)
        |> fetch_flash()
        |> put_flash(:error, "That's not yours!")
        |> redirect(to: Routes.marketing_path(conn, :index))
        |> halt()

      {nil, _} ->
        message = "Source or source_name needed."

        conn
        |> put_status(406)
        |> put_view(LogflareWeb.LogView)
        |> render("index.json", message: message)
        |> halt()
    end
  end
end
