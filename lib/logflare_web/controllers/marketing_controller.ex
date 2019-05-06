defmodule LogflareWeb.MarketingController do
  use LogflareWeb, :controller

  alias Logflare.SystemCounter
  alias Number.Delimit

  @system_counter :total_logs_logged

  def index(conn, _params) do
    {:ok, log_count} = SystemCounter.log_count(@system_counter)
    render(conn, "index.html", log_count: Delimit.number_to_delimited(log_count))
  end

  def big_query(conn, _params) do
    render(conn, "bigquery+datastudio.html")
  end
end
