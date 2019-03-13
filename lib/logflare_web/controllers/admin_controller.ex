defmodule LogflareWeb.AdminController do
  use LogflareWeb, :controller
  import Ecto.Query, only: [from: 2]

  alias Logflare.Repo
  alias Logflare.SourceData

  def dashboard(conn, _params) do
    query =
      from(s in "sources",
        order_by: s.name,
        select: %{
          name: s.name,
          id: s.id,
          token: s.token
        }
      )

    sources =
      for source <- Repo.all(query) do
        log_count = SourceData.get_log_count(source)
        rate = SourceData.get_rate(source)
        {:ok, token} = Ecto.UUID.load(source.token)

        Map.put(source, :log_count, log_count)
        |> Map.put(:rate, rate)
        |> Map.put(:token, token)
      end

    render(conn, "dashboard.html", sources: sources)
  end
end
