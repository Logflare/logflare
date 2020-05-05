defmodule LogflareWeb.Plugs.SetVerifySource do
  use Plug.Builder

  import Plug.Conn
  import Phoenix.Controller
  alias Logflare.{Sources, Source}
  alias LogflareWeb.Router.Helpers, as: Routes

  def call(%{assigns: %{source: %Source{}}} = conn, _opts), do: conn

  def call(%{assigns: %{user: user}, params: params} = conn, _opts) do
    id = params["source_id"] || params["id"]
    token = params["source"] || params["source_id"]
    name = params["source_name"]

    is_api_path = conn.request_path =~ "/logs" or conn.request_path =~ "/api"
    is_browser_path = not is_api_path

    token =
      if is_browser_path || Sources.Cache.valid_source_token_param?(token) do
        token
      else
        nil
      end

    source =
      cond do
        token && is_api_path ->
          Sources.Cache.get_by_and_preload(token: token)

        name && is_api_path ->
          Sources.Cache.get_by_and_preload(name: name)

        id && is_browser_path ->
          Sources.get_by_and_preload(id: id)

        name && is_browser_path ->
          Sources.get_by_and_preload(name: name)

        true ->
          nil
      end

    user_authorized? = &(&1.user_id === user.id || user.admin)

    case {source && user_authorized?.(source), is_api_path} do
      {true, false} ->
        assign(conn, :source, source)

      {true, true} ->
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

      {nil, true} ->
        message = "Source or source_name is nil, empty or not found."

        conn
        |> put_status(406)
        |> put_view(LogflareWeb.LogView)
        |> render("index.json", message: message)
        |> halt()

      {nil, false} ->
        conn
        |> put_status(302)
        |> put_flash(:error, "Source not found!")
        |> redirect(to: Routes.marketing_path(conn, :index))
        |> halt()
    end
  end
end
