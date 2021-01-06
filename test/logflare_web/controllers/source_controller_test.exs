defmodule LogflareWeb.SourceControllerTest do
  @moduledoc false
  import LogflareWeb.Router.Helpers
  use LogflareWeb.ConnCase
  use Mimic

  alias Logflare.Teams
  alias Logflare.{Sources, Repo, LogEvent}
  alias Logflare.Plans.Plan
  alias Logflare.Lql.FilterRule
  alias Logflare.Logs.Validators
  alias Logflare.SavedSearches
  alias Logflare.Logs.RejectedLogEvents
  import Logflare.Factory

  setup_all do
    Sources.Counters.start_link()
    :ok
  end

  setup do
    u1 = insert(:user)
    u2 = insert(:user)
    Teams.create_team(u1, %{name: "u1 team"})
    Teams.create_team(u2, %{name: "u2 team"})

    s1 = insert(:source, public_token: Faker.String.base64(16), user_id: u1.id)
    s2 = insert(:source, user_id: u1.id)
    s3 = insert(:source, user_id: u2.id)

    users = Repo.preload([u1, u2], :sources)

    sources = [s1, s2, s3]

    {:ok, users: users, sources: sources}
  end

  describe "dashboard" do
    setup [:expect_user_plan]

    test "renders dashboard", %{conn: conn, users: [u1, _u2], sources: [s1, s2 | _]} do
      conn =
        conn
        |> login_user(u1)
        |> get("/dashboard")

      dash_sources = Enum.map(conn.assigns.sources, & &1.metrics)
      dash_source_1 = hd(dash_sources)

      source_stat_fields = ~w[avg buffer inserts latest max rate id]a

      sources = conn.assigns.sources
      assert is_list(dash_sources)
      assert source_stat_fields -- Map.keys(dash_source_1) === []
      assert Enum.sort(Enum.map(sources, & &1.id)) == Enum.sort(Enum.map([s1, s2], & &1.id))
      assert html_response(conn, 200) =~ "dashboard"
    end

    test "renders rejected logs page", %{conn: conn, users: [u1, _u2], sources: [s1, _s2 | _]} do
      RejectedLogEvents.ingest(%LogEvent{
        validation_error: Validators.EqDeepFieldTypes.message(),
        params: %{"no_log_entry" => true, "timestamp" => ""},
        source: s1,
        valid?: false,
        ingested_at: NaiveDateTime.utc_now()
      })

      conn =
        conn
        |> login_user(u1)
        |> get("/sources/#{s1.id}/rejected")

      assert html_response(conn, 200) =~ "dashboard"

      assert [
               %LogEvent{
                 validation_error:
                   "Metadata validation error: values with the same field path must have the same type.",
                 params: %{"no_log_entry" => true, "timestamp" => ""},
                 ingested_at: _
               }
             ] = conn.assigns.logs
    end
  end

  describe "update" do
    setup [:expect_user_plan]

    test "returns 200 with valid params", %{conn: conn, users: [u1, _u2], sources: [s1, _s2 | _]} do
      new_name = Faker.String.base64()

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
        |> patch(Routes.source_path(conn, :update, s1.id), params)

      s1_new = Sources.get_by(token: s1.token)

      assert html_response(conn, 302) =~ "redirected"
      assert get_flash(conn, :info) == "Source updated!"
      assert s1_new.name == new_name
      assert s1_new.favorite == true

      conn =
        conn
        |> recycle()
        |> login_user(u1)
        |> get(source_path(conn, :edit, s1.id))

      assert conn.assigns.source.name == new_name

      params = %{
        "id" => s1.id,
        "source" => %{
          "notifications_every" => "100"
        }
      }

      s1_new = Sources.get_by(token: s1.token)
      assert s1_new.notifications_every == 14_400_000
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
      assert get_flash(conn, :error) == "Something went wrong!"
      assert html_response(conn, 406) =~ "Source Name"
    end

    test "returns 200 but doesn't change restricted params", %{
      conn: conn,
      users: [u1, _u2],
      sources: [s1, _s2 | _]
    } do
      nope_token = Faker.UUID.v4()
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
    setup [:expect_user_plan]

    test "renders source for a logged in user", %{conn: conn, users: [u1 | _], sources: [s1 | _]} do
      conn =
        conn
        |> login_user(u1)
        |> get(source_path(conn, :show, s1.id), %{
          "source" => %{
            "name" => Faker.Person.name()
          }
        })

      assert html_response(conn, 200) =~ s1.name
    end

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

    test "returns 404 for non-existing source", %{
      conn: conn,
      users: [_u1, u2 | _],
      sources: [_s1 | _]
    } do
      conn =
        conn
        |> login_user(u2)
        |> get(source_path(conn, :show, 10_000))

      assert html_response(conn, 404) =~ "404"
      assert html_response(conn, 404) =~ "not found"
    end
  end

  describe "create" do
    setup [:expect_user_plan]

    test "returns 200 with valid params", %{conn: conn, users: [u1 | _]} do
      name = Faker.Name.name()

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

      assert get_flash(conn) === %{"error" => "Something went wrong!"}
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

      assert get_flash(conn) === %{"error" => "Something went wrong!"}
    end
  end

  describe "favorite" do
    setup [:expect_user_plan]

    test "returns 200 flipping the value", %{conn: conn, users: [u1 | _], sources: [s1 | _]} do
      conn =
        conn
        |> login_user(u1)
        |> get(source_path(conn, :favorite, Integer.to_string(s1.id)))

      new_s1 = Sources.get_by(id: s1.id)

      assert get_flash(conn, :info) == "Source updated!"
      assert redirected_to(conn, 302) =~ source_path(conn, :dashboard)
      assert new_s1.favorite == not s1.favorite
    end
  end

  describe "public" do
    test "shows a source page", %{conn: conn, sources: [s1 | _]} do
      conn =
        conn
        |> get(source_path(conn, :public, s1.public_token))

      assert html_response(conn, 200) =~ s1.name
    end
  end

  describe "delete" do
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

      {:ok, _counter} = SavedSearches.inc(saved_search.id, tailing?: false)
      {:ok, _counter} = SavedSearches.inc(saved_search.id, tailing?: true)

      conn =
        conn
        |> login_user(u1)
        |> delete(source_path(conn, :del_source_and_redirect, s1.id))

      assert redirected_to(conn, 302) =~ "/dashboard"
      assert is_nil(Sources.get(s1.id))
      assert is_nil(SavedSearches.get(saved_search.id))
    end
  end

  def login_user(conn, u) do
    conn
    |> Plug.Test.init_test_session(%{user_id: u.id})
    |> assign(:user, u)
  end

  def expect_user_plan(_ctx) do
    expect(Logflare.Plans, :get_plan_by_user, fn _ ->
      %Plan{
        stripe_id: "31415"
      }
    end)

    :ok
  end
end
