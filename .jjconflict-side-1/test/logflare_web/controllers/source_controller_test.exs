defmodule LogflareWeb.SourceControllerTest do
  use LogflareWeb.ConnCase

  alias Logflare.Teams
  alias Logflare.Sources
  alias Logflare.Repo
  alias Logflare.Backends.SourceSup
  alias Logflare.Backends
  alias Logflare.SystemMetrics.AllLogsLogged

  setup do
    start_supervised!(AllLogsLogged)
    :ok
  end

  describe "dashboard" do
    setup do
      insert(:plan, name: "Free")
      user = insert(:user)
      home_team = insert(:team, user: user)

      [invited_team1, invited_team2] = insert_pair(:team)
      team_user1 = insert(:team_user, email: user.email, team: invited_team1)
      team_user2 = insert(:team_user, email: user.email, team: invited_team2)

      {:ok,
       user: user,
       home_team: home_team,
       invited_team1: invited_team1,
       invited_team2: invited_team2,
       team_user1: team_user1,
       team_user2: team_user2}
    end

    test "navigating between teams results in correct query params being set", %{
      conn: conn,
      user: user,
      invited_team1: invited_team1
    } do
      conn
      |> login_user(user)
      |> visit(~p"/dashboard?t=#{invited_team1.id}")
      |> then(fn session ->
        html = Floki.parse_document!(session.conn.resp_body)

        links =
          html
          |> Floki.find("a[href*='?t=#{invited_team1.id}']")

        assert Enum.find(links, fn elem -> Floki.text(elem) =~ "Dashboard" end)
      end)
    end
  end

  describe "dashboard admin link in navbar" do
    setup do
      insert(:plan, name: "Free")
      user = insert(:user, admin: true)
      admin_team = insert(:team, user: user)

      # user1 has home team
      user1 = insert(:user)
      insert(:team, user: user1)
      team_user1 = insert(:team_user, email: user1.email, team: admin_team)

      # user2 has no home tema
      team_user2 = insert(:team_user, email: "some@email.com", team: admin_team)

      {:ok, user: user, admin_team: admin_team, team_user1: team_user1, team_user2: team_user2}
    end

    defp nav_and_assert_admin_link(conn, team_name, team_id) do
      conn
      |> visit(~p"/")
      |> then(fn session ->
        try do
          click_link(session, team_name)
        rescue
          _ -> session
        end
      end)
      |> then(fn session ->
        html = Floki.parse_document!(session.conn.resp_body)

        links =
          html
          |> Floki.find("a[href*='?t=#{team_id}']")

        assert Enum.find(links, fn elem -> Floki.text(elem) =~ "Admin" end)
      end)
    end

    test "viewing as invited team_user - no home team", %{
      conn: conn,
      user: user,
      team_user2: team_user,
      admin_team: admin_team
    } do
      conn
      |> login_user(user, team_user)
      |> nav_and_assert_admin_link(admin_team.name, admin_team.id)
    end

    test "viewing as invited team_user - with home team", %{
      conn: conn,
      user: user,
      team_user1: team_user,
      admin_team: admin_team
    } do
      conn
      |> login_user(user, team_user)
      |> nav_and_assert_admin_link(admin_team.name, admin_team.id)
    end

    test "viewing as admin user", %{
      conn: conn,
      user: user,
      admin_team: admin_team
    } do
      conn |> login_user(user) |> nav_and_assert_admin_link(admin_team.name, admin_team.id)
    end
  end

  describe "list" do
    setup %{conn: conn} do
      user = insert(:user)
      insert(:plan, name: "Free")
      team = insert(:team, user: user)
      source = insert(:source, user: user)
      user = Repo.preload(user, :sources)
      [user: user, source: source, team: team, conn: login_user(conn, user)]
    end

    test "show source", %{conn: conn, source: source} do
      conn
      |> visit(~p"/sources/#{source}")
      |> assert_has("h5 > a[href='#{~p"/sources/#{source}"}']", text: source.name)
      |> assert_has("li > a", text: "Sign out", exact: true)
      |> assert_has("button > span", text: "Search", exact: true)
    end

    test "show source's recent logs", %{conn: conn, source: source} do
      start_supervised!({SourceSup, source})
      le = build(:log_event, source: source, metadata: %{"level" => "debug"})
      Backends.ingest_logs([le], source)

      conn
      |> visit(~p"/sources/#{source}")
      |> assert_has("li > a", text: "event body", exact: true)
      |> assert_has("li mark.log-level-debug", text: "debug")
      |> assert_has("pre > code",
        text: Logflare.JSON.encode!(le.body["event_message"], pretty: true)
      )
    end

    test "renders inputs for recommended query fields", %{
      conn: conn,
      user: user
    } do
      source =
        insert(:source,
          user: user,
          suggested_keys: "metadata.level!,m.user_id",
          bigquery_clustering_fields: "session_id"
        )

      conn
      |> visit(~p"/sources/#{source}")
      |> assert_has("label", text: "session_id")
      |> assert_has("label", text: "metadata.level")
      |> assert_has("label", text: "m.user_id")
      |> assert_has(".required-field-indicator", text: "required")
      |> assert_has("input.form-control-sm[id='recent-logs-field-session_id']")
      |> assert_has("input.form-control-sm[id='recent-logs-field-metadata.level']")
      |> assert_has("input.form-control-sm[id='recent-logs-field-m.user_id']")
    end

    test "invalid source", %{conn: conn, source: source} do
      html =
        conn
        |> get(~p"/sources/12345")
        |> html_response(404)

      # main nav
      assert html =~ "Sign out"
      refute html =~ "Sign in"
      # subnav
      refute html =~ source.name
      refute html =~ "scroll down"
      refute html =~ "Search"
    end

    test "forbidden source", %{conn: conn} do
      other_source = insert(:source, user: build(:user))

      html =
        conn
        |> get(~p"/sources/#{other_source}")
        |> html_response(404)

      # main nav
      assert html =~ "Sign out"
      refute html =~ "Sign in"
      # error content
      assert html =~ "404"
      assert html =~ "not found"
    end
  end

  describe "Premium only features" do
    setup %{conn: conn} do
      insert(:plan, name: "Free")
      paid_user = insert(:user, billing_enabled: true)
      plan = insert(:plan, name: "Paid", stripe_id: "stripe-id")
      insert(:billing_account, user: paid_user, stripe_plan_id: plan.stripe_id)
      insert(:team, user: paid_user)

      free_user = insert(:user)
      insert(:team, user: free_user)

      [conn: conn, paid_user: paid_user, free_user: free_user]
    end

    test "can see SMS alert options", %{conn: conn, paid_user: paid_user} do
      source = insert(:source, user: paid_user)

      conn
      |> login_user(paid_user)
      |> visit(~p"/sources/#{source}/edit")
      |> assert_has("button", text: "Update SMS preferences", exact: true)
      |> refute_has("p", text: "SMS alerts are not available with the Free plan.")
    end

    test "free user", %{conn: conn, free_user: free_user} do
      source = insert(:source, user: free_user)

      conn
      |> login_user(free_user)
      |> visit(~p"/sources/#{source}/edit")
      |> refute_has("button", text: "Update SMS preferences", exact: true)
      |> assert_has("p", text: "SMS alerts are not available with the Free plan.")
    end
  end

  describe "edit" do
    setup %{conn: conn} do
      insert(:plan, name: "Free")
      free_user = insert(:user)
      insert(:team, user: free_user)

      [conn: conn, free_user: free_user]
    end

    test "pipeline", %{conn: conn, free_user: user} do
      source = insert(:source, user: user)

      conn
      |> login_user(user)
      |> visit(~p"/sources/#{source}/edit")
      |> assert_has("h3", text: "Pipeline Rules", exact: true)
      |> assert_has("h5", text: "Copy fields")
      |> assert_has("button", text: "Update field copying rules", exact: true)

      conn =
        conn
        |> login_user(user)
        |> patch(~p"/sources/#{source}", %{
          source: %{
            transform_copy_fields: """
            test:123
            123:1234
            """
          }
        })

      assert html_response(conn, 302) =~ "redirected"
      assert Phoenix.Flash.get(conn.assigns.flash, :info) == "Source updated!"

      assert Sources.get_by(id: source.id).transform_copy_fields
    end
  end

  describe "show" do
    setup [:create_plan, :old_setup]

    test "returns 404 for a source not owned by the user", %{
      conn: conn,
      users: [_u1, u2 | _],
      sources: [s1 | _]
    } do
      conn =
        conn
        |> login_user(u2)
        |> get(~p"/sources/#{s1}")

      assert html_response(conn, 404) =~ "not found"
    end
  end

  describe "new" do
    setup [:create_plan]

    setup do
      user = insert(:user)

      team = insert(:team, user: user)
      [user: user, team: team]
    end

    test "logged user can create a new source", %{conn: conn, user: user} do
      conn
      |> login_user(user)
      |> visit(~p"/dashboard")
      |> click_link("New source")
      |> assert_path(~p"/sources/new")
      |> assert_has("h5", text: "~/logs/new")
      |> assert_has("form")
      |> fill_in("Source Name", with: "MyApp.Logs")
      |> submit()
      |> assert_path(~p"/sources/*", query_params: %{new: true})
    end

    test "team user can create a new source", %{conn: conn, user: user} do
      team = insert(:team, name: "Team1")
      team_user = insert(:team_user, email: user.email, team: team)

      conn
      |> login_user(team.user, team_user)
      |> visit(~p"/dashboard?t=#{team.id}")
      |> click_link("New source")
      |> assert_path(~p"/sources/new", query_params: %{t: team.id})
      |> assert_has("h5", text: "~/logs/new")
      |> assert_has("form")
      |> fill_in("Source Name", with: "MyApp.Logs")
      |> submit()
      |> assert_path(~p"/sources/*", query_params: %{new: true})
      |> assert_has("a", href: ~p"/dashboard?t=#{team.id}")
    end

    test "returns 403 for a user not logged in", %{conn: conn} do
      assert conn
             |> visit(~p"/sources/new")
             |> assert_path(~p"/auth/login")
    end
  end

  describe "update" do
    setup [:create_plan, :old_setup]

    test "returns 200 with valid params", %{conn: conn, users: [u1, _u2], sources: [s1, _s2 | _]} do
      new_name = TestUtils.random_string()

      params = %{
        "id" => s1.id,
        "source" => %{
          "favorite" => true,
          "name" => new_name
        }
      }

      conn =
        conn
        |> login_user(u1)
        |> patch(~p"/sources/#{s1}", params)

      s1_new = Sources.get_by(token: s1.token)

      assert html_response(conn, 302) =~ "redirected"
      assert Phoenix.Flash.get(conn.assigns.flash, :info) == "Source updated!"
      assert s1_new.name == new_name
      assert s1_new.favorite == true

      conn =
        conn
        |> recycle()
        |> login_user(u1)
        |> get(~p"/sources/#{s1}")

      assert conn.assigns.source.name == new_name
    end

    test "able to update labels", %{conn: conn, users: [u1, _u2], sources: [s1, _s2 | _]} do
      params = %{
        "id" => s1.id,
        "source" => %{
          "labels" => "test=some_label"
        }
      }

      conn =
        conn
        |> login_user(u1)
        |> patch(~p"/sources/#{s1}", params)

      s1_new = Sources.get_by(token: s1.token)

      assert html_response(conn, 302) =~ "redirected"
      assert Phoenix.Flash.get(conn.assigns.flash, :info) == "Source updated!"
      assert s1_new.labels == "test=some_label"
    end

    test "returns 406 with invalid params", %{
      conn: conn,
      users: [u1, _u2],
      sources: [s1, _s2 | _]
    } do
      new_name = "this should never be inserted"

      params = %{
        "id" => s1.id,
        "source" => %{
          "favorite" => 123,
          "name" => new_name
        }
      }

      conn =
        conn
        |> login_user(u1)
        |> patch(~p"/sources/#{s1}", params)

      s1_new = Sources.get_by(token: s1.token)

      assert s1_new.name != new_name
      assert Phoenix.Flash.get(conn.assigns.flash, :error) == "Something went wrong!"
      assert html_response(conn, 406) =~ "Source Name"
    end

    test "returns 200 but doesn't change restricted params", %{
      conn: conn,
      users: [u1, _u2],
      sources: [s1, _s2 | _]
    } do
      nope_token = TestUtils.gen_uuid()
      nope_api_quota = 1337
      nope_user_id = 1

      params = %{
        "id" => s1.id,
        "source" => %{
          "name" => s1.name,
          "token" => nope_token,
          "api_quota" => nope_api_quota,
          "user_id" => nope_user_id
        }
      }

      conn =
        conn
        |> login_user(u1)
        |> patch(~p"/sources/#{s1}", params)

      s1_new = Sources.get_by(id: s1.id)

      refute conn.assigns[:changeset]
      refute s1_new.token == nope_token
      refute s1_new.api_quota == nope_api_quota
      refute s1_new.user_id == nope_user_id
      assert redirected_to(conn, 302) =~ source_path(conn, :edit, s1.id)
    end

    test "returns 404 when user is not an owner of source", %{
      conn: conn,
      users: [u1, _u2],
      sources: [s1, _s2, u2s1 | _]
    } do
      conn =
        conn
        |> login_user(u1)
        |> patch(
          ~p"/sources/#{u2s1}",
          %{
            "source" => %{
              "name" => "it's mine now!"
            }
          }
        )

      s1_new = Sources.get_by(id: s1.id)

      refute s1_new.name === "it's mine now!"
      assert conn.halted === true
      assert html_response(conn, 404) =~ "not found"
    end
  end

  describe "create" do
    setup [:create_plan, :old_setup]

    test "returns 200 with valid params", %{conn: conn, users: [u1 | _]} do
      name = TestUtils.random_string()

      conn =
        conn
        |> login_user(u1)
        |> post(~p"/sources", %{
          "source" => %{
            "name" => name
          }
        })

      source = Sources.get_by(name: name)

      refute conn.assigns[:changeset]
      assert redirected_to(conn, 302) === source_path(conn, :show, source.id) <> "?new=true"
    end

    test "renders error flash and redirects for missing name", %{conn: conn, users: [u1 | _]} do
      conn =
        conn
        |> login_user(u1)
        |> post(~p"/sources", %{
          "source" => %{
            "name" => ""
          }
        })

      assert conn.assigns[:changeset].errors === [
               name: {"can't be blank", [validation: :required]}
             ]

      assert Phoenix.Flash.get(conn.assigns.flash, :error) === "Something went wrong!"
    end

    test "renders error flash for source with empty name", %{conn: conn, users: [u1 | _]} do
      conn =
        conn
        |> login_user(u1)
        |> post(~p"/sources", %{
          "source" => %{
            "name" => ""
          }
        })

      assert conn.assigns[:changeset].errors === [
               name: {"can't be blank", [validation: :required]}
             ]

      assert Phoenix.Flash.get(conn.assigns.flash, :error) === "Something went wrong!"
    end
  end

  describe "public" do
    setup [:create_plan, :old_setup]

    test "shows a source page", %{conn: conn, sources: [s1 | _]} do
      conn
      |> visit(~p"/sources/public/#{s1.public_token}")
      |> assert_has("h5 > a", text: s1.name)
      |> assert_has("span#source-id", text: to_string(s1.token))
    end
  end

  describe "delete" do
    setup [:create_plan, :old_setup]

    test "deletes a source", %{conn: conn, sources: [s1 | _], users: [u1 | _]} do
      conn
      |> login_user(u1)
      |> visit(~p"/dashboard")
      |> assert_has("a", href: ~p"/sources/#{s1}", text: s1.name)

      assert conn
             |> login_user(u1)
             |> delete(~p"/sources/#{s1}/force-delete")
             |> redirected_to(302) =~ "/dashboard"

      refute Sources.get(s1.id)

      conn
      |> login_user(u1)
      |> visit(~p"/dashboard")
      |> refute_has("a", href: ~p"/sources/#{s1}", text: s1.name)
    end
  end

  describe "update pipeline" do
    setup :create_plan

    setup do
      Logflare.Google.BigQuery
      |> stub(:init_table!, fn _, _, _, _, _, _ -> :ok end)

      on_exit(fn ->
        for {_id, child, _, _} <- DynamicSupervisor.which_children(Backends.SourcesSup) do
          DynamicSupervisor.terminate_child(Backends.SourcesSup, child)
        end
      end)

      [user: insert(:user)]
    end
  end

  describe "team context links" do
    setup %{conn: conn} do
      user = insert(:user)
      insert(:plan, name: "Free")
      team = insert(:team, user: user)
      team_user = insert(:team_user, team: team, email: user.email)
      source = insert(:source, user: user)

      [
        conn: login_user(conn, user, team_user),
        user: user,
        team: team,
        team_user: team_user,
        source: source
      ]
    end

    test "source show page links include team query param", %{
      conn: conn,
      source: source,
      team_user: team_user
    } do
      html =
        conn
        |> get(~p"/sources/#{source}?t=#{team_user.team_id}")
        |> html_response(200)

      for path <- ~w(explore rules clear edit) do
        assert html =~
                 ~r/href="[^"]*\/sources\/#{source.id}\/#{path}[^"]*[?&]t=#{team_user.team_id}/
      end
    end

    test "source search form includes team query param", %{
      conn: conn,
      source: source,
      team_user: team_user
    } do
      html =
        conn
        |> get(~p"/sources/#{source}?t=#{team_user.team_id}")
        |> html_response(200)

      # Check search form has hidden input with team param
      assert html =~ ~r/<input[^>]*name="t"/
    end

    test "source edit page has Rules link with team query param", %{
      conn: conn,
      source: source,
      team_user: team_user
    } do
      html =
        conn
        |> get(~p"/sources/#{source}/edit?t=#{team_user.team_id}")
        |> html_response(200)

      assert html =~ ~r/sources\/#{source.id}\/rules/ or html =~ ~r/RulesLive/
      assert html =~ "?t=#{team_user.team_id}" or html =~ "&amp;t=#{team_user.team_id}"
    end
  end

  defp create_plan(_) do
    insert(:plan, name: "Free")

    :ok
  end

  defp old_setup(_) do
    u1 = insert(:user)
    u2 = insert(:user)
    Teams.create_team(u1, %{name: "u1 team"})
    Teams.create_team(u2, %{name: "u2 team"})

    s1 = insert(:source, public_token: TestUtils.random_string(), user_id: u1.id)
    s2 = insert(:source, user_id: u1.id)
    s3 = insert(:source, user_id: u2.id)

    users = [u1, u2]
    sources = [s1, s2, s3]

    {:ok, users: users, sources: sources}
  end
end
