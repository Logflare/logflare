defmodule LogflareWeb.Plugs.VerifySourceOwner do
  import Plug.Conn
  import Phoenix.Controller

  alias Logflare.Repo
  alias Logflare.Source
  alias LogflareWeb.Router.Helpers, as: Routes

  def init(_params) do
  end

  def call(conn, _opts) do
    source_id = get_source_id(conn)
    user_id = conn.assigns.user.id
    source = Repo.get(Source, source_id)

    continue(conn, user_id, source)
  end

  defp get_source_id(conn) do
    cond do
      conn.params["source_id"] ->
        conn.params["source_id"]

      conn.params["id"] ->
        conn.params["id"]

      true ->
        nil
    end
  end

  defp continue(conn, _user_id, source) when is_nil(source) do
    conn
    |> put_flash(:error, "That's not yours!")
    |> redirect(to: Routes.source_path(conn, :index))
    |> halt()
  end

  defp continue(conn, user_id, source) when is_nil(source) == false do
    case user_id == source.user_id do
      true ->
        conn

      false ->
        conn
        |> put_flash(:error, "That's not yours!")
        |> redirect(to: Routes.source_path(conn, :index))
        |> halt()
    end
  end
end
