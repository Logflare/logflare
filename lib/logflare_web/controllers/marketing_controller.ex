defmodule LogflareWeb.MarketingController do
  use LogflareWeb, :controller

  alias Logflare.SystemMetrics.AllLogsLogged
  alias Number.Delimit

  @system_counter :total_logs_logged
  @announcement %{
    message: "Logflare is now part of Supabase.",
    cta_text: "Read more →",
    cta_link: "https://supabase.com/blog/supabase-acquires-logflare?utm_source=logflare-site&utm_medium=referral&utm_campaign=logflare-acquired"
  }

  # only set the banner assigns on marketing pages
  plug :assign, {:banner, @announcement}

  def index(conn, _params) do
    {:ok, log_count} = AllLogsLogged.log_count(@system_counter)
    render(conn, "index.html", log_count: Delimit.number_to_delimited(log_count))
  end

  def contact(conn, _params) do
    render(conn, "contact.html")
  end

  def pricing(conn, _params) do
    render(conn, "pricing.html")
  end

  def terms(conn, _params) do
    render(conn, "terms_of_service.html")
  end

  def privacy(conn, _params) do
    render(conn, "privacy_policy.html")
  end

  def cookies(conn, _params) do
    render(conn, "cookie_policy.html")
  end

  def guides(conn, _params) do
    render(conn, "guides.html")
  end

  def overview(conn, _params) do
    render(conn, "overview.html")
  end

  def vercel_setup(conn, _params) do
    render(conn, "vercel_setup.html")
  end

  def big_query_setup(conn, _params) do
    render(conn, "bigquery_setup.html")
  end

  def slack_app_setup(conn, _params) do
    render(conn, "slack_app_setup.html")
  end

  def data_studio_setup(conn, _params) do
    render(conn, "data_studio_setup.html")
  end

  def event_analytics_demo(conn, _params) do
    render(conn, "event_analytics_demo.html")
  end
end
