defmodule LogflareWeb.AdminController do
  use LogflareWeb, :controller
  import Ecto.Query, only: [from: 2]

  alias Logflare.{Repo, Source, Sources}

  @page_size 50
  @default_sort_by :latest

  def dashboard(conn, params) do
    sort_options = [:schema_fields, :latest, :rejected]
    sorted_sources = sorted_sources(params)

    render(conn, "dashboard.html", sources: sorted_sources, sort_options: sort_options)
  end

  defp sorted_sources(%{"page" => page, "sort_by" => sort_by} = _params) do
    query()
    |> Repo.all()
    |> Enum.map(&Sources.preload_defaults/1)
    |> Enum.map(&put_schema_field_count/1)
    |> Enum.sort_by(&Map.fetch(&1.metrics, String.to_atom(sort_by)), &>=/2)
    |> Repo.paginate(%{page_size: @page_size, page: page})
  end

  defp sorted_sources(%{"sort_by" => sort_by} = _params) do
    query()
    |> Repo.all()
    |> Enum.map(&Sources.preload_defaults/1)
    |> Enum.map(&put_schema_field_count/1)
    |> Enum.sort_by(&Map.fetch(&1.metrics, String.to_atom(sort_by)), &>=/2)
    |> Repo.paginate(%{page_size: @page_size, page: 1})
  end

  defp sorted_sources(_params) do
    query()
    |> Repo.all()
    |> Enum.map(&Sources.preload_defaults/1)
    |> Enum.map(&put_schema_field_count/1)
    |> Enum.sort_by(&Map.fetch(&1.metrics, @default_sort_by), &>=/2)
    |> Repo.paginate(%{page_size: @page_size, page: 1})
  end

  defp query() do
    from s in Source,
      order_by: s.name,
      select: s
  end

  defp put_schema_field_count(source) do
    new_metrics =
      source.metrics
      |> Map.put(:schema_fields, Source.Data.get_schema_field_count(source))

    %{source | metrics: new_metrics}
  end
end
