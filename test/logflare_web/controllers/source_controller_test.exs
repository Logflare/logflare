defmodule LogflareWeb.SourceControllerTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  import LogflareWeb.Router.Helpers

  alias Logflare.Teams
  alias Logflare.Sources
  alias Logflare.Repo
  alias Logflare.LogEvent
  alias Logflare.Billing.Plan
  alias Logflare.Lql.FilterRule
  alias Logflare.Logs.Validators
  alias Logflare.SavedSearches
  alias Logflare.Logs.RejectedLogEvents
  alias Logflare.SingleTenant

  describe "list" do
    setup %{conn: conn} do
      Logflare.Sources.Counters
      |> stub()
      |> stub(:get_inserts, fn _token -> {:ok, 123} end)
      |> stub(:get_bq_inserts, fn _token -> {:ok, 456} end)

      user = insert(:user)
      insert(:plan, name: "Free")
      insert(:team, user: user)
      source = insert(:source, user: user)
      user = Repo.preload(user, :sources)
      [source: source, conn: login_user(conn, user)]
    end

    test "renders dashboard", %{conn: conn, source: source} do
      html =
        conn
        |> get(Routes.source_path(conn, :dashboard))
        |> html_response(200)

      # nav
      assert html =~ "~/logs"
      assert html =~ "Saved Searches"
      assert html =~ "Dashboard"
      assert html =~ source.name
    end

    test "show source", %{conn: conn, source: source} do
      html =
        conn
        |> get(Routes.source_path(conn, :show, source))
        |> html_response(200)

      # main nav
      assert html =~ "Sign out"
      # subnav
      assert html =~ source.name
      assert html =~ "scroll down"
      # search
      assert html =~ "Search"
    end

    test "invalid source", %{conn: conn, source: source} do
      html =
        conn
        |> get(Routes.source_path(conn, :show, 12_345))
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
        |> get(Routes.source_path(conn, :show, other_source))
        |> html_response(403)

      # main nav
      assert html =~ "Sign out"
      refute html =~ "Sign in"
      # error content
      assert html =~ "403"
      assert html =~ "Forbidden"
    end
  end

  describe "Premium only features" do
    setup %{conn: conn} do
      # mocks
      Logflare.Sources.Counters
      |> stub()
      |> stub(:get_inserts, fn _token -> {:ok, 123} end)
      |> stub(:get_bq_inserts, fn _token -> {:ok, 456} end)

      # setup paid plan
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
      paid_user = Repo.preload(paid_user, :sources)

      html =
        conn
        |> login_user(paid_user)
        |> get(Routes.source_path(conn, :edit, source))
        |> html_response(200)

      assert html =~ "Update SMS preferences"
      refute html =~ "SMS alerts are not available with the Free plan"
    end

    test "free user", %{conn: conn, free_user: free_user} do
      source = insert(:source, user: free_user)
      free_user = Repo.preload(free_user, :sources)

      html =
        conn
        |> login_user(free_user)
        |> get(Routes.source_path(conn, :edit, source))
        |> html_response(200)

      # cannot see update button
      refute html =~ "Update SMS preferences"
      # can see alert
      assert html =~ "SMS alerts are not available with the Free plan"
    end
  end

  describe "dashboard single tenant" do
    TestUtils.setup_single_tenant(seed_user: true)

    setup do
      Logflare.Sources.Counters
      |> stub()
      |> stub(:get_inserts, fn _token -> {:ok, 123} end)
      |> stub(:get_bq_inserts, fn _token -> {:ok, 456} end)

      [user: SingleTenant.get_default_user()]
    end

    test "renders source in dashboard", %{conn: conn, user: user} do
      source = insert(:source, user: user)

      html =
        conn
        |> get(Routes.source_path(conn, :dashboard))
        |> html_response(200)

      assert html =~ source.name
    end

    test "renders source page", %{conn: conn, user: user} do
      source = insert(:source, user: user)

      html =
        conn
        |> get(Routes.source_path(conn, :show, source))
        |> html_response(200)

      assert html =~ source.name
    end
  end

  describe "dashboard - rejected" do
    setup [:old_setup, :expect_user_plan, :assert_caches_not_called]

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
        |> get("/sources/#{s1.id}/rejected")

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

  describe "update" do
    setup [:old_setup, :expect_user_plan, :assert_caches_not_called]

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
        |> patch("/sources/#{s1.id}", params)

      s1_new = Sources.get_by(token: s1.token)

      assert html_response(conn, 302) =~ "redirected"
      assert Phoenix.Flash.get(conn.assigns.flash, :info) == "Source updated!"
      assert s1_new.name == new_name
      assert s1_new.favorite == true

      conn =
        conn
        |> recycle()
        |> login_user(u1)
        |> get(source_path(conn, :edit, s1.id))

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
          "favorite" => 1,
          "name" => new_name
        }
      }

      conn =
        conn
        |> login_user(u1)
        |> patch("/sources/#{s1.id}", params)

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
        |> patch("/sources/#{s1.id}", params)

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
          "/sources/#{u2s1.id}",
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

  describe "show" do
    setup [:old_setup, :expect_user_plan, :assert_caches_not_called]

    test "returns 403 for a source not owned by the user", %{
      conn: conn,
      users: [_u1, u2 | _],
      sources: [s1 | _]
    } do
      conn =
        conn
        |> login_user(u2)
        |> get(source_path(conn, :show, s1.id))

      assert html_response(conn, 403) =~ "403"
      assert html_response(conn, 403) =~ "Forbidden"
    end
  end

  describe "create" do
    setup [:old_setup, :expect_user_plan, :assert_caches_not_called]

    test "returns 200 with valid params", %{conn: conn, users: [u1 | _]} do
      name = TestUtils.random_string()

      conn =
        conn
        |> login_user(u1)
        |> post("/sources", %{
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
        |> post("/sources", %{
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
        |> post("/sources", %{
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
    setup [:old_setup, :expect_user_plan, :assert_caches_not_called]

    test "returns 200 flipping the value", %{conn: conn, users: [u1 | _], sources: [s1 | _]} do
      conn =
        conn
        |> login_user(u1)
        |> get(source_path(conn, :favorite, Integer.to_string(s1.id)))

      new_s1 = Sources.get_by(id: s1.id)

      assert Phoenix.Flash.get(conn.assigns.flash, :info) == "Source updated!"
      assert redirected_to(conn, 302) =~ source_path(conn, :dashboard)
      assert new_s1.favorite == not s1.favorite
    end
  end

  describe "public" do
    setup [:old_setup]

    test "shows a source page", %{conn: conn, sources: [s1 | _]} do
      conn =
        conn
        |> get(source_path(conn, :public, s1.public_token))

      assert html_response(conn, 200) =~ s1.name
    end
  end

  describe "delete" do
    setup [:old_setup]

    test "deletes a source", %{conn: conn, sources: [s1 | _], users: [u1 | _]} do
      {:ok, saved_search} =
        SavedSearches.insert(
          %{
            querystring: "error",
            lql_rules: [
              %FilterRule{
                operator: :=,
                value: "error",
                modifiers: %{},
                path: "event_message"
              }
            ]
          },
          s1
        )

      {:ok, _counter} = SavedSearches.inc(saved_search.id, tailing: false)
      {:ok, _counter} = SavedSearches.inc(saved_search.id, tailing: true)

      conn =
        conn
        |> login_user(u1)
        |> delete(source_path(conn, :del_source_and_redirect, s1.id))

      assert redirected_to(conn, 302) =~ "/dashboard"
      assert is_nil(Sources.get(s1.id))
      assert is_nil(SavedSearches.get(saved_search.id))
    end
  end

  defp old_setup(_) do
    Sources.Counters.start_link()

    insert(:plan, name: "Free")
    u1 = insert(:user)
    u2 = insert(:user)
    Teams.create_team(u1, %{name: "u1 team"})
    Teams.create_team(u2, %{name: "u2 team"})

    s1 = insert(:source, public_token: TestUtils.random_string(), user_id: u1.id)
    s2 = insert(:source, user_id: u1.id)
    s3 = insert(:source, user_id: u2.id)

    users = Repo.preload([u1, u2], :sources)

    sources = [s1, s2, s3]

    {:ok, users: users, sources: sources}
  end

  defp expect_user_plan(_ctx) do
    expect(Logflare.Billing, :get_plan_by_user, fn _ ->
      %Plan{
        stripe_id: "31415"
      }
    end)

    :ok
  end

  defp assert_caches_not_called(_) do
    reject(&Sources.Cache.get_by/1)
    :ok
  end
end
