defmodule LogflareWeb.Api.SourceController do
  use LogflareWeb, :controller
  alias Logflare.Sources
  alias Logflare.Users

  action_fallback LogflareWeb.Api.FallbackController

  def index(%{assigns: %{user: user}} = conn, _) do
    user = Users.preload_sources(user)
    sources = Sources.preload_for_dashboard(user.sources)
    json(conn, sources)
  end

  def create(%{assigns: %{user: user}} = conn, params) do
    with params <- Map.put(params, "token", Ecto.UUID.generate()),
         {:ok, _} <- Sources.create_source(params, user) do
      conn
      |> Plug.Conn.send_resp(204, [])
      |> Plug.Conn.halt()
    end
  end

  def update(%{assigns: %{user: user}} = conn, %{"id" => id} = params) do
    with source when not is_nil(source) <- Sources.get_by(id: id, user_id: user.id),
         {:ok, _} <- Sources.update_source_by_user(source, params) do
      conn
      |> Plug.Conn.send_resp(204, [])
      |> Plug.Conn.halt()
    end
  end

  def delete(%{assigns: %{user: user}} = conn, %{"id" => id}) do
    with source when not is_nil(source) <- Sources.get_by(id: id, user_id: user.id),
         {:ok, _} <- Sources.delete_source(source) do
      conn
      |> Plug.Conn.send_resp(204, [])
      |> Plug.Conn.halt()
    end
  end
end
