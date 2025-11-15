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
      conn = conn |> login_user(user)
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

  describe "saved searches" do
    setup %{source: source} do
      [saved_search: insert(:saved_search, source: source)]
    end

    test "render saved searches", %{conn: conn, saved_search: saved_search} do
      {:ok, _view, html} = live(conn, "/dashboard")

      assert html =~ "Saved Searches"
      assert html =~ saved_search.querystring
    end

    test "delete saved search ", %{conn: conn, saved_search: saved_search} do
      {:ok, view, html} = live(conn, "/dashboard")

      assert html =~ saved_search.querystring

      view
      |> element("[phx-click='delete_saved_search'][phx-value-id='#{saved_search.id}']")
      |> render_click()

      {:ok, _view, html} = live(conn, "/dashboard")

      refute html =~ saved_search.querystring
    end

    test "shows error when deleting non-existent saved search", %{conn: conn} do
      {:ok, view, _html} = live(conn, "/dashboard")

      assert render_hook(view, "delete_saved_search", %{"id" => "999999"}) =~
               "Saved search not found"
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

  describe "dashboard - viewing home team as team member" do
    setup %{user: user, conn: conn} do
      other_team = insert(:team, name: "Other Team")
      forbidden_team = insert(:team, name: "Not My Team")

      team_user = insert(:team_user, team: other_team)
      other_member = insert(:team_user, team: user.team)

      conn = conn |> login_user(user, team_user)

      [
        other_team: other_team,
        forbidden_team: forbidden_team,
        other_member: other_member,
        team_user: team_user,
        conn: conn
      ]
    end

    test "teams list", %{conn: conn, other_team: other_team} do
      {:ok, view, _html} = live(conn, "/dashboard")

      assert view |> has_element?("#teams li", "#{other_team.name}")
    end

    test "team members list", %{conn: conn, user: user, other_member: other_member} do
      {:ok, view, _html} = live(conn, "/dashboard")

      refute view |> element("#members li", "#{user.name}") |> render =~ "owner, you"
      assert view |> has_element?("#members li", "#{other_member.name}")
    end
  end

  describe "displaying source metrics" do
    test "starts UserMetricsPoller when session has string user_id", %{user: user} do
      # Simulate what happens when session data is deserialized with string user_id
      conn =
        build_conn()
        |> Plug.Test.init_test_session(%{user_id: "#{user.id}"})
        |> Plug.Conn.assign(:user, user)

      {:ok, _view, _html} = live(conn, "/dashboard")

      # The UserMetricsPoller should be registered and running after mount
      assert {poller_pid, _} = :syn.lookup(:core, {Logflare.Sources.UserMetricsPoller, user.id})
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

      assert view |> element("li[id=source-#{source.token}] [title^=Pipelines]") |> render =~
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

      assert {poller_pid, _} = :syn.lookup(:core, {Logflare.Sources.UserMetricsPoller, user.id})

      send(poller_pid, :poll_metrics)
      # wait for broadcast
      Process.sleep(100)

      assert view |> element("li[id=source-#{source.token}] [title^=Pipelines]") |> render =~
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
end
