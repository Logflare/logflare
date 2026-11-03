defmodule LogflareWeb.Backends.QueryClassSettingsLiveTest do
  use LogflareWeb.ConnCase

  alias Logflare.Backends
  alias Logflare.Repo

  setup %{conn: conn} do
    insert(:plan)
    owner = insert(:user, admin: false)
    team = insert(:team, user: owner)

    backend =
      insert(:backend,
        user: owner,
        type: :clickhouse,
        config: %{url: "http://localhost:8123", port: 8123, database: "default"}
      )

    %{conn: login_user(conn, owner), owner: owner, team: team, backend: backend}
  end

  test "owner saves defaults and class overrides independently of backend config", context do
    {:ok, view, html} = live_with_redirect(context.conn, ~p"/backends/#{context.backend.id}/edit")
    assert html =~ "Query-class settings"
    assert html =~ "Updates propagate eventually"

    reject(Backends, :update_backend, 2)

    view
    |> form("#query-class-settings-form",
      query_class_settings: %{
        "default" => %{"priority" => "5", "max_threads" => "8", "max_execution_time" => "2.5"},
        "api_paid" => %{"priority" => "10", "max_threads" => "4", "max_rows_to_read" => "1000"}
      }
    )
    |> render_submit()

    assert has_element?(view, "[role=status]", "Query-class settings saved")
    saved = Backends.get_backend(context.backend.id)

    assert saved.config.query_class_settings == %{
             "default" => %{
               "priority" => 5,
               "max_threads" => 8,
               "max_execution_time" => 2.5,
               "timeout_overflow_mode" => "throw"
             },
             "api_paid" => %{
               "priority" => 10,
               "max_threads" => 4,
               "max_rows_to_read" => 1000,
               "read_overflow_mode" => "throw"
             }
           }

    assert saved.name == context.backend.name
    assert saved.config.url == "http://localhost:8123"

    {:ok, reloaded, _html} = live_with_redirect(context.conn, ~p"/backends/#{saved.id}/edit")
    assert has_element?(reloaded, "#query-class-default_priority[value='5']")
    assert has_element?(reloaded, "#query-class-api_paid_max_threads[value='4']")
  end

  test "invalid policy is rejected without losing entered values or stored policy", context do
    {:ok, _backend} =
      Backends.configure_query_class_settings(context.owner, context.backend, %{
        "default" => %{"priority" => 5}
      })

    {:ok, view, _html} =
      live_with_redirect(context.conn, ~p"/backends/#{context.backend.id}/edit")

    view
    |> form("#query-class-settings-form",
      query_class_settings: %{
        "default" => %{"priority" => "0"},
        "api_paid" => %{"max_threads" => "4"}
      }
    )
    |> render_submit()

    assert has_element?(view, "#query-class-settings-error")
    assert has_element?(view, "#query-class-default_priority[value='0']")
    assert has_element?(view, "#query-class-api_paid_max_threads[value='4']")

    assert Backends.get_backend(context.backend.id).config.query_class_settings == %{
             "default" => %{"priority" => 5}
           }
  end

  test "blank overrides inherit defaults and disable clears only class policy", context do
    {:ok, _backend} =
      Backends.configure_query_class_settings(context.owner, context.backend, %{
        "default" => %{"priority" => 5},
        "api_paid" => %{"max_threads" => 4}
      })

    endpoint =
      insert(:endpoint,
        user: context.owner,
        backend: context.backend,
        language: :ch_sql,
        enforced_clickhouse_settings: %{"max_threads" => 2}
      )

    {:ok, view, _html} =
      live_with_redirect(context.conn, ~p"/backends/#{context.backend.id}/edit")

    view
    |> form("#query-class-settings-form",
      query_class_settings: %{"api_paid" => %{"max_threads" => ""}}
    )
    |> render_submit()

    assert Backends.get_backend(context.backend.id).config.query_class_settings == %{
             "default" => %{"priority" => 5}
           }

    view |> element("button", "Disable query-class policy") |> render_click()
    assert has_element?(view, "[role=status]", "Query-class policy disabled")
    assert Backends.get_backend(context.backend.id).config.query_class_settings == %{}
    assert Repo.reload!(endpoint).enforced_clickhouse_settings == %{"max_threads" => 2}
  end

  test "existing non-form policy fields survive a structured edit", context do
    {:ok, _backend} =
      Backends.configure_query_class_settings(context.owner, context.backend, %{
        "default" => %{
          "priority" => 5,
          "read_from_filesystem_cache_if_exists_otherwise_bypass_cache" => 1
        }
      })

    {:ok, view, _html} =
      live_with_redirect(context.conn, ~p"/backends/#{context.backend.id}/edit")

    view
    |> form("#query-class-settings-form",
      query_class_settings: %{"default" => %{"priority" => "6"}}
    )
    |> render_submit()

    assert Backends.get_backend(context.backend.id).config.query_class_settings["default"] == %{
             "priority" => 6,
             "read_from_filesystem_cache_if_exists_otherwise_bypass_cache" => 1
           }
  end

  test "team members can edit, but revoked membership cannot save through an open view",
       context do
    member = insert(:team_user, team: context.team)
    conn = login_user(context.conn, context.owner, member)
    {:ok, view, _html} = live_with_redirect(conn, ~p"/backends/#{context.backend.id}/edit")

    view
    |> form("#query-class-settings-form",
      query_class_settings: %{"default" => %{"priority" => "5"}}
    )
    |> render_submit()

    assert has_element?(view, "[role=status]", "Query-class settings saved")
    Repo.delete!(member)

    view
    |> form("#query-class-settings-form",
      query_class_settings: %{"default" => %{"priority" => "10"}}
    )
    |> render_submit()

    assert has_element?(view, "#query-class-settings-error", "You no longer have permission")

    assert Backends.get_backend(context.backend.id).config.query_class_settings == %{
             "default" => %{"priority" => 5}
           }
  end

  test "another account cannot open the editor", context do
    outsider = insert(:user)
    insert(:team, user: outsider)

    assert_raise LogflareWeb.ErrorsLive.InvalidResourceError, fn ->
      context.conn
      |> login_user(outsider)
      |> live_with_redirect(~p"/backends/#{context.backend.id}/edit")
    end
  end

  test "non-ClickHouse backends do not display the editor", context do
    backend = insert(:backend, user: context.owner, type: :bigquery)
    {:ok, view, _html} = live_with_redirect(context.conn, ~p"/backends/#{backend.id}/edit")
    refute has_element?(view, "#query-class-settings-form")
  end
end
