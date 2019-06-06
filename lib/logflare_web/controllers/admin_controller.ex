defmodule LogflareWeb.AdminController do
  use LogflareWeb, :controller
  import Ecto.Query, only: [from: 2]

  alias Logflare.{Repo, Source, Sources}

  def dashboard(conn, _params) do
    query =
      from s in Source,
        order_by: s.name,
        select: %Source{
          name: s.name,
          id: s.id,
          token: s.token
        }

    sorted_sources =
      query
      |> Repo.all()
      |> Enum.map(&Sources.preload_defaults/1)
      |> Enum.sort_by(&Map.fetch(&1.metrics, :latest), &>=/2)

    render(conn, "dashboard.html", sources: sorted_sources)
  end
end
