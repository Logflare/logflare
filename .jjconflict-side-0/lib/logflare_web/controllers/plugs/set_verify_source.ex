defmodule LogflareWeb.Plugs.SetVerifySource do
  @moduledoc """
  Verifies user ownership of a source for browser only
  """
  use Plug.Builder

  import Plug.Conn
  import Phoenix.Controller

  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.User
  alias LogflareWeb.Plugs.SetTeamContext

  def call(%{assigns: %{source: %Source{}}} = conn, _opts), do: conn

  def call(%{request_path: "/sources/public/" <> public_token} = conn, opts) do
    set_source_for_public(public_token, conn, opts)
  end

  def call(%{assigns: %{user: %User{admin: true}}, params: params} = conn, _opts) do
    id = params["source_id"] || params["id"]
    source = Sources.get_by_and_preload(id: id)

    assign(conn, :source, source)
  end

  def call(%{assigns: assigns, params: params} = conn, _opts) do
    id = params["source_id"] || params["id"]
    current_email = get_session(conn, :current_email)
    effective_user = assigns[:team_user] || assigns[:user]

    case Sources.get_by_user_access(effective_user, id) do
      source = %Source{} ->
        source =
          source
          |> Sources.preload_defaults()
          |> Sources.put_retention_days()

        conn
        |> assign(:source, source)
        |> SetTeamContext.set_team_context(source.user.team.id, current_email)

      nil ->
        conn
        |> put_status(404)
        |> put_layout(false)
        |> put_view(LogflareWeb.ErrorView)
        |> render("404_page.html")
        |> halt()
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
end
