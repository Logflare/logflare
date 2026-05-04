defmodule LogflareWeb.MarketingController do
  use LogflareWeb, :controller

  alias Logflare.SystemMetrics.AllLogsLogged
  alias Number.Delimit
  alias Logflare.SingleTenant

  @system_counter :total_logs_logged
  @announcement %{
    message: "Logflare is now part of Supabase.",
    cta_text: "Read more →",
    cta_link:
      "https://supabase.com/blog/supabase-acquires-logflare?utm_source=logflare-site&utm_medium=referral&utm_campaign=logflare-acquired"
  }

  # only set the banner assigns on marketing pages
  plug :assign, {:banner, @announcement}
  plug :single_tenant_redirect

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
    conn
    |> put_status(301)
    |> redirect(external: "https://supabase.com/terms")
  end

  def privacy(conn, _params) do
    conn
    |> put_status(301)
    |> redirect(external: "https://supabase.com/privacy")
  end

  def guides(conn, _params) do
    conn
    |> put_status(301)
    |> redirect(external: "https://docs.logflare.app")
    |> halt()
  end

  def overview(conn, _params) do
    conn
    |> put_status(301)
    |> redirect(external: "https://docs.logflare.app")
    |> halt()
  end

  def vercel_setup(conn, _params) do
    conn
    |> put_status(301)
    |> redirect(external: "https://docs.logflare.app/integrations/vercel/")
    |> halt()
  end

  def big_query_setup(conn, _params) do
    conn
    |> put_status(301)
    |> redirect(
      external:
        "https://docs.logflare.app/backends/bigquery/#setting-up-your-own-bigquery-backend"
    )
    |> halt()
  end

  def slack_app_setup(conn, _params) do
    conn
    |> put_status(301)
    |> redirect(external: "https://docs.logflare.app/alerts/slack")
    |> halt()
  end

  def data_studio_setup(conn, _params) do
    conn
    |> put_status(301)
    |> redirect(
      external: "https://docs.logflare.app/backends/bigquery/#data-studio-looker-integration"
    )
    |> halt()
  end

  def event_analytics_demo(conn, _params) do
    conn
    |> put_status(301)
    |> redirect(external: "https://docs.logflare.app")
    |> halt()
  end

  defp single_tenant_redirect(conn, _) do
    if SingleTenant.single_tenant?() do
      conn
      |> redirect(to: "/dashboard")
      |> halt()
    else
      conn
    end
  end
end
