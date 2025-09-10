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
    conn = conn |> put_session(:user_id, user.id)

    {:ok, user: user, source: source, conn: conn}
  end

  describe "Dashboard Live" do
    test "renders dashboard", %{conn: conn, source: source} do
      {:ok, view, html} = live(conn, "/dashboard")

      assert view |> has_element?("h5", "~/logs")
      assert html =~ source.name
    end
  end

  describe "dashboard single tenant" do
    TestUtils.setup_single_tenant(seed_user: true)

    setup %{conn: conn} do
      user =
        Logflare.SingleTenant.get_default_user()

      insert(:team, user: user)
      conn = conn |> put_session(:user_id, user.id) |> assign(:user, user)
      [user: user, conn: conn]
    end

    test "renders source in dashboard", %{conn: conn, user: user} do
      source = insert(:source, user: user)

      {:ok, view, _} = live(conn, "/dashboard")
      assert view |> has_element?(~s|a[href="/sources/#{source.id}"]|, source.name)
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

    test "renders saved searches", %{conn: conn, source: source} do
      {:ok, _view, html} = live(conn, "/dashboard")

      assert html =~ "Saved Searches"
      assert html =~ "test query"
      assert html =~ source.name
    end
  end

  describe "dashboard - home team" do
    setup %{user: user} do
      other_team = insert(:team, name: "Other Team")
      forbidden_team = insert(:team, name: "Not My Team")

      team_user = insert(:team_user, team: other_team, email: user.email)
      other_member = insert(:team_user, team: user.team)

      [
        other_team: other_team,
        forbidden_team: forbidden_team,
        other_member: other_member,
        team_user: team_user
      ]
    end

    test "teams list", %{
      conn: conn,
      user: user,
      other_team: other_team,
      forbidden_team: forbidden_team
    } do
      {:ok, view, _html} = live(conn, "/dashboard")

      assert view |> has_element?("#teams li", "#{user.team.name}home team")
      assert view |> has_element?("#teams a", other_team.name)
      refute view |> has_element?("#teams a", forbidden_team.name)
    end

    test "team members list", %{conn: conn, user: user, other_member: other_member} do
      {:ok, view, _html} = live(conn, "/dashboard")

      assert view |> element("#members li", "#{user.name}") |> render =~ "owner, you"
      assert view |> has_element?("#members li", "#{other_member.name}")
    end

    test "sign in to other team", %{
      conn: conn,
      user: user,
      other_team: other_team,
      other_member: other_member,
      team_user: team_user,
      forbidden_team: forbidden_team
    } do
      {:ok, view, _html} = live(conn, "/dashboard")

      {:ok, conn} =
        view
        |> element("a", other_team.name)
        |> render_click()
        |> follow_redirect(
          conn,
          ~p"/profile/switch?#{%{team_user_id: team_user, user_id: other_team.user_id}}"
        )

      {:ok, view, _html} = live(conn, "/dashboard")

      assert view |> has_element?("#teams span", other_team.name)
      assert view |> has_element?("#teams a", user.team.name)
      refute view |> has_element?("#teams a", forbidden_team.name)

      assert view |> element("#members li", other_team.user.name) |> render =~ "owner"
      refute view |> has_element?("#members li", other_member.name)
    end
  end

  describe "team member management" do
    setup %{user: user} do
      member = insert(:team_user, team: user.team)
      [member: member]
    end

    test "removes team member", %{conn: conn, member: member} do
      {:ok, view, _html} = live(conn, "/dashboard")

      assert view
             |> has_element?(~s|a[href="/profile/#{member.id}"][data-method="delete"]|)

      delete(conn, ~p"/profile/#{member.id}")

      {:ok, view, _html} = live(conn, "/dashboard")

      refute view |> has_element?("#members li", "#{member.name}")
    end
  end

  describe "displaying source metrics" do
    test "renders source metrics ", %{conn: conn, source: source} do
      {:ok, view, _html} = live(conn, "/dashboard")

      assert view |> has_element?("[id^=#{source.id}-inserts]", "0")
      assert view |> has_element?("span[id=#{source.token}-rate]", "0/s")
      assert view |> has_element?("span[id=#{source.token}-avg-rate]", "0")
      assert view |> has_element?("span[id=#{source.token}-max-rate]", "0")
      assert view |> has_element?("span[id=#{source.token}-rejected]", "0")
      assert view |> element("li[id=#{source.token}] [title^=Pipelines]") |> render =~ "0"
      assert view |> element("li[id=#{source.token}]") |> render =~ "ttl: 3 days"
    end

    test "updates source metrics", %{conn: conn, source: source} do
      {:ok, view, _html} = live(conn, "/dashboard")

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
