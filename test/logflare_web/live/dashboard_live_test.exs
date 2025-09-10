defmodule LogflareWeb.DashboardLiveTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  alias Logflare.Repo
  alias Logflare.Sources.Source

  setup %{conn: conn} do
    insert(:plan)
    user = insert(:user)
    team = insert(:team, user: user)
    source = insert(:source, user: user)
    user = %{user | team: team}
    conn = conn |> put_session(:user_id, user.id) |> assign(:user, user)

    {:ok, user: user, source: source, conn: conn}
  end

  describe "Dashboard Live" do
    test "renders dashboard", %{conn: conn} do
      {:ok, view, html} = live(conn, "/dashboard")

      assert view |> has_element?("h5", "~/logs")
    end

    test "show source", %{conn: conn, source: source} do
      {:ok, view, html} = live(conn, "/dashboard")

      assert html =~ source.name
    end
  end

  describe "favoriting a source" do
    test "favorite a source", %{conn: conn, source: source} do
      {:ok, view, _} = live(conn, "/dashboard")

      refute source.favorite
      assert view |> element(".favorite .far") |> has_element?()

      view
      |> element("##{source.token} .favorite")
      |> render_click()

      updated_source = source |> Repo.reload()
      assert updated_source.favorite

      assert view |> element(".favorite .fas") |> has_element?()
    end

    test "unfavorite a source", %{conn: conn, source: source} do
      {:ok, favorited_source} = source |> Logflare.Sources.update_source(%{favorite: true})
      {:ok, view, _html} = live(conn, "/dashboard")

      assert favorited_source.favorite
      assert view |> element(".favorite .fas") |> has_element?()

      view
      |> element(".favorite")
      |> render_click()

      updated_source = favorited_source |> Repo.reload()
      refute updated_source.favorite
      assert view |> has_element?(".favorite .far")
    end
  end

  describe "saved searches" do
    setup %{source: source} do
      {:ok, saved_search} =
        Logflare.SavedSearches.insert(
          %{
            lql_rules: [],
            querystring: "test query",
            saved_by_user: true,
            tailing: true
          },
          source
        )

      [saved_search: saved_search]
    end

    test "renders saved searches", %{conn: conn, source: source, saved_search: saved_search} do
      {:ok, _view, html} = live(conn, "/dashboard")

      assert html =~ "Saved Searches"
      assert html =~ "test query"
      assert html =~ source.name
    end
  end
  describe "displaying source metrics" do
    test "renders source metrics ", %{conn: conn, source: source} do
      {:ok, view, html} = live(conn, "/dashboard")

      assert view |> has_element?("[id^=#{source.id}-inserts]", "0")
      assert view |> has_element?("span[id=#{source.token}-rate]", "0/s")
      assert view |> has_element?("span[id=#{source.token}-avg-rate]", "0")
      assert view |> has_element?("span[id=#{source.token}-max-rate]", "0")
      assert view |> has_element?("span[id=#{source.token}-rejected]", "0")
      assert view |> element("li[id=#{source.token}] [title^=Pipelines]") |> render =~ "0"
      assert view |> element("li[id=#{source.token}]") |> render =~ "ttl: 3 days"
    end

    test "updates source metrics", %{conn: conn, user: user, source: source} do
      {:ok, view, html} = live(conn, "/dashboard")

      buffer = :rand.uniform(100)
      log_count = :rand.uniform(100)

      rates_payload = %{
        average_rate: :rand.uniform(100),
        max_rate: :rand.uniform(100),
        last_rate: :rand.uniform(100),
        source_token: source.token
      }

      Source.ChannelTopics.local_broadcast_buffer(%{
        buffer: buffer,
        source_id: source.id,
        backend_id: nil
      })

      Source.ChannelTopics.local_broadcast_log_count(%{
        log_count: log_count,
        source_token: source.token
      })

      Source.ChannelTopics.local_broadcast_rates(rates_payload)

      assert view |> element("li[id=#{source.token}] [title^=Pipelines]") |> render =~
               to_string(buffer)

      assert view |> has_element?("span[id=#{source.token}-rate]", "#{rates_payload.last_rate}/s")

      assert view
             |> has_element?(
               "span[id=#{source.token}-avg-rate]",
               to_string(rates_payload.average_rate)
             )

      assert view
             |> has_element?(
               "span[id=#{source.token}-max-rate]",
               to_string(rates_payload.max_rate)
             )

      assert view |> has_element?("[id^=#{source.id}-inserts]", to_string(log_count))
    end
  end
end
