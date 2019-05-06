defmodule LogflareWeb.Plugs.VerifySourceOwner do
  use Plug.Builder

  import Plug.Conn
  import Phoenix.Controller

  alias Logflare.Repo
  alias Logflare.Source
  alias LogflareWeb.Router.Helpers, as: Routes

  plug(:verify_owner)

  def verify_owner(conn, _opts) do
    source_id = get_source_id(conn)
    source = Repo.get(Source, source_id)

    continue(conn, source)
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

  defp continue(conn, source) when is_nil(source) do
    conn
    |> put_flash(:error, "That's not yours!")
    |> redirect(to: Routes.marketing_path(conn, :index))
    |> halt()
  end

  defp continue(conn, source) when is_nil(source) == false do
    cond do
      conn.assigns.user.admin ->
        conn

      conn.assigns.user.id == source.user_id ->
        conn

      true ->
        conn
        |> put_flash(:error, "That's not yours!")
        |> redirect(to: Routes.marketing_path(conn, :index))
        |> halt()
    end
  end
end
