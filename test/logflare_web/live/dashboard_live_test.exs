defmodule LogflareWeb.DashboardLiveTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  alias Logflare.Repo

  setup %{conn: conn} do
    insert(:plan)
    user = insert(:user)
    team = insert(:team, user: user)
    source = insert(:source, user: user)
    user = %{user | team: team}
    conn = conn |> login_user(user)

    {:ok, user: user, source: source, conn: conn, team: team}
  end

  describe "Dashboard Live" do
    setup {TestUtils, :attach_wait_for_render}

    test "renders dashboard", %{conn: conn, source: source} do
      {:ok, view, html} = live(conn, "/dashboard")

      assert view |> has_element?("h5", "~/logs")
      assert html =~ source.name
    end

    test "renders source description value", %{conn: conn, user: user} do
      description = "Production API logs"
      source = insert(:source, user: user, description: description)

      {:ok, view, _html} = live(conn, "/dashboard")

      assert view
             |> element("#source-#{source.token}")
             |> render() =~ description
    end

    test "truncates long source descriptions and exposes the full value in a tooltip", %{
      conn: conn,
      user: user
    } do
      description = String.trim(String.duplicate("Long source description ", 20))
      source = insert(:source, user: user, description: description)

      {:ok, view, _html} = live(conn, "/dashboard")

      rendered =
        view
        |> element("#source-#{source.token}")
        |> render()
        |> Floki.parse_fragment!()

      assert rendered
             |> Floki.find("#source-#{source.token}-description")
             |> Floki.text()
             |> String.trim()
             |> String.length() == 281

      assert rendered
             |> Floki.find("#source-#{source.token}-description")
             |> Floki.attribute("data-title") == [description]
    end

    test "sources have a saved searches modal", %{conn: conn, source: source} do
      {:ok, view, _html} = live(conn, "/dashboard")

      refute view |> has_element?("#saved-searches-modal")

      view
      |> element("#source-#{source.token} a[phx-click='show_live_modal']")
      |> render_click()

      Logflare.TestUtils.wait_for_render(view, "#saved-searches-modal")

      assert view |> has_element?(".modal-title", "Saved Searches")
    end
  end

  describe "dashboard single tenant" do
    TestUtils.setup_single_tenant(seed_user: true)

    setup %{conn: conn} do
      user =
        Logflare.SingleTenant.get_default_user()

      team = insert(:team, user: user)
      conn = conn |> login_user(user)
      [user: user, conn: conn, team: team]
    end

    test "renders source in dashboard", %{conn: conn, user: user, team: team} do
      source = insert(:source, user: user)

      {:ok, _view, html} = live(conn, "/dashboard")

      assert html =~ source.name
      assert html =~ ~r/sources\/#{source.id}[^"<]*t=#{team.id}/
    end
  end

  describe "favoriting a source" do
    test "favorite a source", %{conn: conn, source: source} do
      {:ok, view, _} = live(conn, "/dashboard")

      refute source.favorite
      assert view |> element(".favorite .far") |> has_element?()

      view
      |> element("#source-#{source.token} .favorite")
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

  describe "dashboard - viewing home team as user" do
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

    test "team members list", %{conn: conn, user: user, other_member: other_member, team: team} do
      {:ok, view, _html} = live(conn, "/dashboard")

      assert view |> element("#members li", "#{user.name}") |> render =~ "owner, you"
      assert view |> has_element?("#members li", "#{other_member.name}")

      assert view
             |> has_element?(
               "a[href='/account/edit?t=#{team.id}#team-members']",
               "Invite more team members"
             )
    end
  end

  describe "dashboard - viewing home team as team member" do
    setup %{conn: conn} do
      other_team = insert(:team, name: "Other Team")
      forbidden_team = insert(:team, name: "Not My Team")

      team_user = insert(:team_user, team: other_team)
      another_member = insert(:team_user, team: other_team)

      # Login as the team_user (using unique email from factory)
      conn =
        conn
        |> Plug.Test.init_test_session(%{current_email: team_user.email})

      [
        other_team: other_team,
        forbidden_team: forbidden_team,
        another_member: another_member,
        team_user: team_user,
        conn: conn
      ]
    end

    test "team members list", %{
      conn: conn,
      other_team: other_team,
      another_member: another_member
    } do
      {:ok, view, _html} = live(conn, "/dashboard")

      assert view |> has_element?("#members li", "#{other_team.user.name}")
      assert view |> has_element?("#members li", "#{another_member.name}")

      refute view
             |> has_element?("a[href='/account/edit#team-members']", "Invite more team members")
    end
  end

  describe "displaying source metrics" do
    test "starts UserMetricsPoller when session has string user_id", %{user: user, conn: conn} do
      # Simulate what happens when session data is deserialized with string user_id
      conn =
        conn
        |> Plug.Test.init_test_session(%{user_id: "#{user.id}"})
        |> Plug.Conn.assign(:user, user)

      {:ok, _view, _html} = live(conn, "/dashboard")

      # The UserMetricsPoller should be registered and running after mount
      assert {poller_pid, _} = :syn.lookup(:ui, {Logflare.Sources.UserMetricsPoller, user.id})
      assert Process.alive?(poller_pid)

      # Should have one subscriber (the LiveView process)
      assert [_subscriber] = Logflare.Sources.UserMetricsPoller.list_subscribers(user.id)
    end

    test "renders source metrics ", %{conn: conn, source: source} do
      {:ok, view, _html} = live(conn, "/dashboard")

      assert view |> has_element?("[id^=source-#{source.token}-inserts]", "0")
      assert view |> has_element?("span[id=#{source.token}-rate]", "0/s")
      assert view |> has_element?("span[id=#{source.token}-avg-rate]", "0")
      assert view |> has_element?("span[id=#{source.token}-max-rate]", "0")
      assert view |> has_element?("span[id=#{source.token}-rejected]", "0")

      assert view |> element("li[id=source-#{source.token}] [data-title^=Pipelines]") |> render =~
               "0"

      assert view |> element("li[id=source-#{source.token}]") |> render =~ "ttl: 3 days"
    end

    test "updates source metrics", %{conn: conn, source: source, user: user} do
      buffer = :rand.uniform(100)
      log_count = :rand.uniform(100)
      avg_rate = :rand.uniform(100)
      max_rate = :rand.uniform(100)
      last_rate = :rand.uniform(100)

      Logflare.Cluster.Utils
      |> stub(:rpc_multicall, fn
        Logflare.PubSubRates.Cache, :get_all_local_metrics, [user_id] when user_id == user.id ->
          sources = Logflare.Sources.list_sources_by_user(user_id)

          node_metrics =
            Enum.reduce(sources, %{}, fn source, acc ->
              Map.put(acc, source.token, %{
                rates: %{average_rate: avg_rate, last_rate: last_rate, max_rate: max_rate},
                buffer: %{len: buffer},
                inserts: %{"node" => %{bq_inserts: log_count, node_inserts: 0}}
              })
            end)

          {
            [node_metrics],
            []
          }
      end)

      {:ok, view, _html} = live(conn, "/dashboard")

      assert {poller_pid, _} = :syn.lookup(:ui, {Logflare.Sources.UserMetricsPoller, user.id})

      send(poller_pid, :poll_metrics)
      # wait for broadcast
      Process.sleep(100)

      assert view |> element("li[id=source-#{source.token}] [data-title^=Pipelines]") |> render =~
               to_string(buffer)

      assert view |> has_element?("span[id=#{source.token}-rate]", "#{last_rate}/s")

      assert view
             |> has_element?(
               "span[id=#{source.token}-avg-rate]",
               to_string(avg_rate)
             )

      assert view
             |> has_element?(
               "span[id=#{source.token}-max-rate]",
               to_string(max_rate)
             )

      assert view |> has_element?("[id^=source-#{source.token}-inserts]", to_string(log_count))
    end
  end

  describe "team query param preservation" do
    setup %{conn: conn} do
      user = insert(:user)
      team = insert(:team, user: user)
      team_user = insert(:team_user, team: team, email: user.email)
      source = insert(:source, user: user)
      saved_search = insert(:saved_search, source: source)

      conn = login_user(conn, user, team_user)

      [
        user: user,
        team: team,
        team_user: team_user,
        source: source,
        saved_search: saved_search,
        conn: conn
      ]
    end

    test "dashboard links preserve team param for home team", %{
      conn: conn,
      source: source,
      team: team
    } do
      {:ok, _view, html} = live(conn, ~p"/dashboard?t=#{team.id}")

      for path <- ["sources/#{source.id}", "sources/#{source.id}/edit", "billing/edit", "account"] do
        assert html =~ ~r/t=#{team.id}/
        assert html =~ "/#{path}"
      end
    end

    test "dashboard links preserve team param", %{
      conn: conn,
      team_user: team_user,
      source: source
    } do
      {:ok, _view, html} = live(conn, ~p"/dashboard?t=#{team_user.team_id}")

      for path <- [
            "sources/#{source.id}",
            "sources/#{source.id}/edit",
            "billing/edit",
            "account"
          ] do
        assert html =~ ~r/#{path}[^"<]*t=#{team_user.team_id}/
      end
    end
  end
end
