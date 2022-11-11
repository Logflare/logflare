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

  def show(%{assigns: %{user: user}} = conn, %{"token" => token}) do
    with source when not is_nil(source) <- Sources.get_by(token: token, user_id: user.id),
         [source] <- Sources.preload_for_dashboard([source]) do
      json(conn, source)
    end
  end

  def create(%{assigns: %{user: user}} = conn, params) do
    with {:ok, source} <- Sources.create_source(params, user) do
      conn
      |> put_status(201)
      |> json(source)
    end
  end

  def update(%{assigns: %{user: user}} = conn, %{"token" => token} = params) do
    with source when not is_nil(source) <- Sources.get_by(token: token, user_id: user.id),
         {:ok, source} <- Sources.update_source_by_user(source, params) do
      conn
      |> put_status(204)
      |> json(source)
    end
  end

  def delete(%{assigns: %{user: user}} = conn, %{"token" => token}) do
    with source when not is_nil(source) <- Sources.get_by(token: token, user_id: user.id),
         {:ok, _} <- Sources.delete_source(source) do
      conn
      |> Plug.Conn.send_resp(204, [])
      |> Plug.Conn.halt()
    end
  end
end
