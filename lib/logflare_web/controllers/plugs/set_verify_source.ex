defmodule LogflareWeb.Plugs.SetVerifySource do
  @moduledoc """
  Verifies user ownership of a source for browser only
  """
  use Plug.Builder

  import Plug.Conn
  import Phoenix.Controller

  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias LogflareWeb.Api.FallbackController

  def call(%{assigns: %{source: %Source{}}} = conn, _opts), do: conn

  def call(%{request_path: "/sources/public/" <> public_token} = conn, opts) do
    set_source_for_public(public_token, conn, opts)
  end

  def call(%{assigns: %{user: user, teams: _teams}, params: params} = conn, _opts) do
    id = params["source_id"] || params["id"]
    source = Sources.get_by_and_preload(id: id)

    cond do
      is_nil(source) ->
        FallbackController.call(conn, {:error, :not_found})

      verify_source_for_user(source, user) ->
        assign(conn, :source, source)

      true ->
        FallbackController.call(conn, {:error, :forbidden})
    end
  end

  defp set_source_for_public(public_token, conn, _opts) do
    case Sources.Cache.get_by_and_preload(public_token: public_token) do
      nil ->
        conn
        |> put_status(404)
        |> put_layout(false)
        |> put_view(LogflareWeb.ErrorView)
        |> render("404_page.html")
        |> halt()

      source ->
        assign(conn, :source, source)
    end
  end

  def verify_source_for_user(source, user) do
    source.user_id == user.id || user.admin
  end
end
