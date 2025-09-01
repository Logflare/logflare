defmodule LogflareWeb.SourceControllerTest do
  use LogflareWeb.ConnCase

  alias Logflare.Teams
  alias Logflare.Sources
  alias Logflare.Repo
  alias Logflare.LogEvent
  alias Logflare.Logs.Validators
  alias Logflare.Logs.RejectedLogEvents
  alias Logflare.SingleTenant
  alias Logflare.Sources.Source.V1SourceDynSup
  alias Logflare.Backends
  alias Logflare.Backends.SourceSup
  alias Logflare.SystemMetrics.AllLogsLogged

  setup do
    start_supervised!(AllLogsLogged)
    :ok
  end

  describe "list" do
    setup %{conn: conn} do
      user = insert(:user)
      insert(:plan, name: "Free")
      team = insert(:team, user: user)
      source = insert(:source, user: user)
      user = Repo.preload(user, :sources)
      [source: source, team: team, conn: login_user(conn, user)]
    end

    test "renders dashboard", %{conn: conn, source: source, team: team} do
      conn
      |> visit(~p"/dashboard")
      |> assert_has("li > a", text: "Dashboard", exact: true)
      |> assert_has("h5", text: "Saved Searches", exact: true)
      |> assert_has("a[href='#{~p"/sources/#{source}"}']", text: source.name)
      |> assert_has("h5", text: "Teams")
      |> assert_has("li", text: team.name)
    end

    test "renders dashboard when user is member of another team", %{conn: conn, source: source} do
      user = conn.assigns.user
      team_user = insert(:team_user, email: user.email, provider_uid: user.provider_uid)

      conn
      |> put_session(:team_user_id, team_user.id)
      |> visit(~p"/dashboard")
      |> assert_has("li > a", text: "Dashboard", exact: true)
      |> assert_has("h5", text: "Saved Searches", exact: true)
      |> assert_has("a[href='#{~p"/sources/#{source}"}']", text: source.name)
      |> assert_has("h5", text: "Teams")
      |> assert_has(
        "a[href='#{~p"/profile/switch?#{%{"user_id" => team_user.team.user_id, "team_user_id" => team_user.id}}"}']",
        text: team_user.team.name
      )
    end

    test "renders default plan ttl correctly", %{conn: conn} do
      conn
      |> visit(~p"/dashboard")
      |> assert_has("small", text: "ttl: 3 days")
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
        |> html_response(403)

      # main nav
      assert html =~ "Sign out"
      refute html =~ "Sign in"
      # error content
      assert html =~ "403"
      assert html =~ "Forbidden"
    end
  end

  test "prompt to switch team if not found and part of many teams", %{conn: conn} do
    hidden_team = insert(:team, user: build(:user))

    insert(:plan)
    user = insert(:user)
    source = insert(:source, user: user)
    main_team = insert(:team, user: user)

    other_user = insert(:user)
    other_team = insert(:team, user: other_user)
    insert(:team_user, team: main_team, provider_uid: other_user.provider_uid)

    # main team has 2 users now
    html =
      conn
      |> login_user(other_user)
      |> get(~p"/sources/#{source.id}")
      |> html_response(403)

    assert html =~ other_team.name
    assert html =~ main_team.name
    refute html =~ hidden_team.name
    assert html =~ "You may need to switch teams"
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

  describe "dashboard single tenant" do
    TestUtils.setup_single_tenant(seed_user: true)

    setup do
      [user: SingleTenant.get_default_user()]
    end

    test "renders source in dashboard", %{conn: conn, user: user} do
      source = insert(:source, user: user)

      conn
      |> visit(~p"/dashboard")
      |> assert_has("a", text: source.name, href: ~p"/sources/#{source}")
    end
  end

  describe "dashboard - rejected" do
    setup [:create_plan, :old_setup]

    test "renders rejected logs page", %{conn: conn, users: [u1, _u2], sources: [s1, _s2 | _]} do
      RejectedLogEvents.ingest(%LogEvent{
        pipeline_error: %LogEvent.PipelineError{message: Validators.EqDeepFieldTypes.message()},
        params: %{"no_log_entry" => true, "timestamp" => ""},
        source: s1,
        valid: false,
        ingested_at: NaiveDateTime.utc_now()
      })

      conn =
        conn
        |> login_user(u1)
        |> get(~p"/sources/#{s1}/rejected")

      assert html_response(conn, 200) =~ "dashboard"

      assert [
               %LogEvent{
                 pipeline_error: %LogEvent.PipelineError{
                   message:
                     "Validation error: values with the same field path must have the same type."
                 },
                 params: %{"no_log_entry" => true, "timestamp" => ""},
                 ingested_at: _
               }
             ] = conn.assigns.logs
    end
  end

  describe "show" do
    setup [:create_plan, :old_setup]

    test "returns 403 for a source not owned by the user", %{
      conn: conn,
      users: [_u1, u2 | _],
      sources: [s1 | _]
    } do
      conn =
        conn
        |> login_user(u2)
        |> get(~p"/sources/#{s1}")

      assert html_response(conn, 403) =~ "Forbidden"
    end
  end

  describe "new" do
    setup [:create_plan]

    test "logged user can create a new source", %{conn: conn} do
      user = insert(:user)
      Teams.create_team(user, %{name: "Test Team"})

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

    test "returns 403 when user is not an owner of source", %{
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
      assert html_response(conn, 403) =~ "Forbidden"
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

  describe "favorite" do
    setup [:create_plan, :old_setup]

    test "returns 200 flipping the value", %{conn: conn, users: [u1 | _], sources: [s1 | _]} do
      conn =
        conn
        |> login_user(u1)
        |> get(~p"/sources/#{s1}/favorite")

      new_s1 = Sources.get_by(id: s1.id)

      assert Phoenix.Flash.get(conn.assigns.flash, :info) == "Source updated!"
      assert redirected_to(conn, 302) =~ source_path(conn, :dashboard)
      assert new_s1.favorite == not s1.favorite
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

  describe "update v1-v2 pipeline" do
    setup :create_plan

    setup do
      Logflare.Google.BigQuery
      |> stub(:init_table!, fn _, _, _, _, _, _ -> :ok end)

      on_exit(fn ->
        for dynsup <- [V1SourceDynSup, Backends.SourcesSup],
            {_id, child, _, _} <- DynamicSupervisor.which_children(dynsup) do
          DynamicSupervisor.terminate_child(dynsup, child)
        end
      end)

      [user: insert(:user)]
    end

    test "toggling from v1 to v2", %{conn: conn, user: user} do
      source = insert(:source, user: user)

      assert conn
             |> login_user(user)
             |> put(~p"/sources/#{source.id}", %{"source" => %{"v2_pipeline" => true}})
             |> redirected_to(302) == ~p"/sources/#{source}/edit"
    end

    test "toggle from v2 to v1", %{conn: conn, user: user} do
      source = insert(:source, user: user, v2_pipeline: true)

      assert conn
             |> login_user(user)
             |> put(~p"/sources/#{source}", %{"source" => %{"v2_pipeline" => false}})
             |> redirected_to(302) == ~p"/sources/#{source}/edit"
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
