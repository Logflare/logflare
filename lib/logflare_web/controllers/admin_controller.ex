defmodule LogflareWeb.AdminController do
  use LogflareWeb, :controller
  import Ecto.Query, only: [from: 2]

  alias Logflare.{Repo, Source, Sources}

  @page_size 50

  def dashboard(conn, params) do
    sort_options = [
      :fields,
      :latest,
      :rejected,
      :rate,
      :avg,
      :max,
      :buffer,
      :inserts,
      :recent
    ]

    sorted_sources = sorted_sources(params)

    render(conn, "dashboard.html", sources: sorted_sources, sort_options: sort_options)
  end

  defp sorted_sources(%{"page" => page, "sort_by" => sort_by} = _params) do
    query()
    |> Repo.all()
    |> Stream.map(&Sources.refresh_source_metrics/1)
    |> Stream.map(&Sources.put_schema_field_count/1)
    |> Enum.sort_by(&Map.fetch(&1.metrics, String.to_atom(sort_by)), &>=/2)
    |> Repo.paginate(%{page_size: @page_size, page: page})
  end

  defp sorted_sources(%{"sort_by" => sort_by} = _params) do
    query()
    |> Repo.all()
    |> Stream.map(&Sources.refresh_source_metrics/1)
    |> Stream.map(&Sources.put_schema_field_count/1)
    |> Enum.sort_by(&Map.fetch(&1.metrics, String.to_atom(sort_by)), &>=/2)
    |> Repo.paginate(%{page_size: @page_size, page: 1})
  end

  defp sorted_sources(_params) do
    query()
    |> Repo.all()
    |> Stream.map(&Sources.refresh_source_metrics/1)
    |> Stream.map(&Sources.put_schema_field_count/1)
    |> Enum.into([])
    |> Repo.paginate(%{page_size: @page_size, page: 1})
  end

  defp query() do
    from s in Source,
      order_by: [desc: s.inserted_at],
      select: s
  end
end
