defmodule LogflareWeb.MarketingController do
  use LogflareWeb, :controller

  alias Logflare.SystemMetrics.AllLogsLogged
  alias Number.Delimit

  @system_counter :total_logs_logged

  def index(conn, _params) do
    {:ok, log_count} = AllLogsLogged.log_count(@system_counter)
    render(conn, "index.html", log_count: Delimit.number_to_delimited(log_count))
  end

  def big_query(conn, _params) do
    render(conn, "bigquery_datastudio.html")
  end

  def big_query_setup(conn, _params) do
    render(conn, "bigquery_setup.html")
  end

  def data_studio_setup(conn, _params) do
    render(conn, "data_studio_setup.html")
  end

  def event_analytics_demo(conn, _params) do
    render(conn, "event_analytics_demo.html")
  end

  def terms(conn, _params) do
    conn
    |> redirect(external: "https://github.com/Logflare/logflare/blob/master/legal/terms.md")
  end

  def privacy(conn, _params) do
    conn
    |> redirect(external: "https://github.com/Logflare/logflare/blob/master/legal/privacy.md")
  end

  def cookies(conn, _params) do
    conn
    |> redirect(external: "https://github.com/Logflare/logflare/blob/master/legal/cookies.md")
  end
end
