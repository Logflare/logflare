defmodule LogflareWeb.AdminController do
  use LogflareWeb, :controller
  import Ecto.Query, only: [from: 2]

  alias Logflare.Repo
  alias Logflare.SourceData
  alias Number.Delimit

  def dashboard(conn, _params) do
    query =
      from s in Source,
        order_by: s.name,
        select: %Source{
          name: s.name,
          id: s.id,
          token: s.token
        }

    sources =
      for source <- Repo.all(query) do
        {:ok, token} = Ecto.UUID.Atom.load(source.token)

        rate = Delimit.number_to_delimited(SourceData.get_rate(source))
        timestamp = SourceData.get_latest_date(source)
        average_rate = Delimit.number_to_delimited(SourceData.get_avg_rate(source))
        max_rate = Delimit.number_to_delimited(SourceData.get_max_rate(source))
        buffer_count = Delimit.number_to_delimited(SourceData.get_buffer(token))
        event_inserts = Delimit.number_to_delimited(SourceData.get_total_inserts(token))

        source
        |> Map.put(:rate, rate)
        |> Map.put(:token, token)
        |> Map.put(:latest, timestamp)
        |> Map.put(:avg, average_rate)
        |> Map.put(:max, max_rate)
        |> Map.put(:buffer, buffer_count)
        |> Map.put(:inserts, event_inserts)
      end

    sorted_sources = Enum.sort_by(sources, &Map.fetch(&1, :latest), &>=/2)

    render(conn, "dashboard.html", sources: sorted_sources)
  end
end
