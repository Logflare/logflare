defmodule LogflareWeb.AdminController do
  use LogflareWeb, :controller
  import Ecto.Query, only: [from: 2]

  alias Logflare.{Repo, Source, Sources}

  def dashboard(conn, params) do
    page_size = 50
    sorted_sources = sorted_sources(page_size, params)

    render(conn, "dashboard.html", sources: sorted_sources)
  end

  defp sorted_sources(page_size, %{"page" => page} = params) do
    query
    |> Repo.all()
    |> Enum.map(&Sources.preload_defaults/1)
    |> Enum.sort_by(&Map.fetch(&1.metrics, :latest), &>=/2)
    |> Repo.paginate(%{page_size: page_size, page: page})
  end

  defp sorted_sources(page_size, params) do
    query
    |> Repo.all()
    |> Enum.map(&Sources.preload_defaults/1)
    |> Enum.sort_by(&Map.fetch(&1.metrics, :latest), &>=/2)
    |> Repo.paginate(%{page_size: page_size, page: 1})
  end

  defp query() do
    from s in Source,
      order_by: s.name,
      select: s
  end
end
