defmodule LogflareWeb.Api.SourceController do
  use LogflareWeb, :controller
  alias Logflare.Sources
  alias Logflare.Users

  def index(%{assigns: %{user: user}} = conn, _) do
    user = Users.preload_sources(user)
    sources = Sources.preload_for_dashboard(user.sources)
    json(conn, sources)
  end
end
