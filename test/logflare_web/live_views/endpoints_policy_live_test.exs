defmodule LogflareWeb.EndpointsPolicyLiveTest do
  use LogflareWeb.ConnCase

  alias Logflare.Endpoints
  alias Logflare.Repo

  setup do
    insert(:plan)
    owner = insert(:user, endpoints_beta: true)
    team = insert(:team, user: owner)
    backend = insert(:backend, user: owner, type: :clickhouse)

    endpoint =
      insert(:endpoint,
        user: owner,
        backend: backend,
        language: :ch_sql,
        sandboxable: true,
        query: "WITH data AS (SELECT 1 AS n) SELECT n FROM data"
      )

    %{owner: owner, team: team, endpoint: endpoint}
  end

  test "ordinary saves display committed data and warn when invalidation fails", context do
    {:ok, view, _} =
      context.conn
      |> login_user(context.owner)
      |> live_with_redirect(~p"/endpoints/#{context.endpoint.id}/edit")

    expect(Endpoints, :update_query, fn actor, endpoint, params, origin ->
      {:ok, updated} =
        Mimic.call_original(Endpoints, :update_query, [actor, endpoint, params, origin])

      {:error, {:saved, updated, :cache_invalidation_failed}}
    end)

    html =
      view
      |> element("form#endpoint")
      |> render_submit(%{endpoint: %{name: "saved despite cache failure"}})

    assert html =~ "Changes saved, but cached results could not be invalidated."
    assert html =~ "saved despite cache failure"
    refute html =~ "Successfully updated"
    assert Endpoints.get_endpoint_query(context.endpoint.id).name == "saved despite cache failure"
  end

  test "restores refresh version history and warn when invalidation fails", context do
    insert(:endpoint_version,
      endpoint: context.endpoint,
      version_number: 1,
      snapshot_overrides: %{"query" => "SELECT 2 AS n"}
    )

    insert(:endpoint_version, endpoint: context.endpoint, version_number: 2)

    {:ok, view, _} =
      context.conn
      |> login_user(context.owner)
      |> live(~p"/endpoints/#{context.endpoint.id}/versions?t=#{context.team}")

    expect(Endpoints, :restore_query_version, fn actor, endpoint, version, origin ->
      {:ok, updated, _version} =
        Mimic.call_original(Endpoints, :restore_query_version, [actor, endpoint, version, origin])

      {:error, {:saved, updated, :cache_invalidation_failed}}
    end)

    render_click(view, "restore-version", %{"version-number" => "1"})
    html = render_async(view, 5_000)
    assert html =~ "Version restored, but cached results could not be invalidated."
    refute html =~ "Unable to restore endpoint version."
    assert Endpoints.get_endpoint_query(context.endpoint.id).query == "SELECT 2 AS n"
    assert Endpoints.get_endpoint_query_version(context.endpoint.id, 3)
    assert html =~ "Show version 3"
  end

  test "members do not inherit their owner's admin flag, even with a colliding user ID",
       context do
    owner = context.owner |> Ecto.Changeset.change(admin: true) |> Repo.update!()
    member = insert(:team_user, id: owner.id, team: context.team)
    assert Logflare.Users.get_by(email: member.email) == nil

    {:ok, view, _} =
      context.conn
      |> login_user(owner, member)
      |> live_with_redirect("/endpoints/#{context.endpoint.id}/edit?t=#{context.team.id}",
        bypass_team_param: true
      )

    refute has_element?(view, "#enforced-settings-form")

    assert render_hook(view, "save-enforced-settings", %{
             "settings" => %{"max_execution_time" => "7"}
           }) =~ "Not authorized to configure ClickHouse settings"

    assert Endpoints.get_endpoint_query(context.endpoint.id).enforced_clickhouse_settings == %{}

    other = insert(:endpoint, language: :ch_sql)

    {:ok, _view, html} =
      context.conn
      |> login_user(owner, member)
      |> live_with_redirect("/endpoints/#{other.id}/edit?t=#{context.team.id}",
        bypass_team_param: true
      )

    assert html =~ "Endpoint Not Found"
  end

  test "team actors cannot configure policy even when their email belongs to an admin", context do
    admin = insert(:user, admin: true)
    member = insert(:team_user, team: context.team, email: admin.email)

    {:ok, view, _} =
      context.conn
      |> login_user(context.owner, member)
      |> live_with_redirect("/endpoints/#{context.endpoint.id}/edit?t=#{context.team.id}",
        bypass_team_param: true
      )

    refute has_element?(view, "#enforced-settings-form")

    assert render_hook(view, "save-enforced-settings", %{
             "settings" => %{"max_execution_time" => "7"}
           }) =~ "Not authorized to configure ClickHouse settings"

    assert Repo.reload!(context.endpoint).enforced_clickhouse_settings == %{}
  end

  test "team actors with admin emails cannot inspect unrelated customer SQL", context do
    admin = insert(:user, admin: true)
    member = insert(:team_user, team: context.team, email: admin.email)
    other = insert(:endpoint, language: :ch_sql, query: "SELECT 1 AS private_customer_sql")

    {:ok, view, html} =
      context.conn
      |> login_user(context.owner, member)
      |> live_with_redirect("/endpoints/#{other.id}/edit?t=#{context.team.id}",
        bypass_team_param: true
      )

    assert html =~ "Endpoint Not Found"
    refute html =~ "private_customer_sql"
    refute has_element?(view, "#enforced-settings-form")
    assert Repo.reload!(other).enforced_clickhouse_settings == %{}
  end

  test "admins use their own account context and lose access when revoked", context do
    admin = insert(:user, admin: true)

    {:ok, view, _} =
      context.conn
      |> login_user(admin)
      |> live_with_redirect("/endpoints/#{context.endpoint.id}/edit")

    assert has_element?(view, "#enforced-settings-form")
    save_settings(view)

    assert Endpoints.get_endpoint_query(context.endpoint.id).enforced_clickhouse_settings == %{
             "max_execution_time" => 7,
             "timeout_overflow_mode" => "throw"
           }

    admin |> Ecto.Changeset.change(admin: false) |> Repo.update!()

    assert render_hook(view, "save-enforced-settings", %{
             "settings" => %{"max_execution_time" => "9"}
           }) =~ "Not authorized to configure ClickHouse settings"

    assert Endpoints.get_endpoint_query(context.endpoint.id).enforced_clickhouse_settings == %{
             "max_execution_time" => 7,
             "timeout_overflow_mode" => "throw"
           }
  end

  test "an unrelated admin cannot open the customer query test page", context do
    admin = insert(:user, admin: true)

    {:ok, view, html} =
      context.conn |> login_user(admin) |> live_with_redirect("/endpoints/#{context.endpoint.id}")

    assert html =~ "Endpoint Not Found"
    reject(ClickHouseAdaptor, :execute_query, 3)

    assert render_hook(view, "run-query", %{"run" => %{"query" => "SELECT 1"}}) =~
             "Not authorized to run this endpoint"
  end

  for sandbox? <- [false, true] do
    @tag sandbox?: sandbox?
    test "an unrelated admin cannot forge query execution from edit (sandbox: #{sandbox?})",
         context do
      admin = insert(:user, admin: true)

      {:ok, view, _} =
        context.conn
        |> login_user(admin)
        |> live_with_redirect("/endpoints/#{context.endpoint.id}/edit")

      assert has_element?(view, "#enforced-settings-form")
      refute has_element?(view, "form#endpoint")
      reject(ClickHouseAdaptor, :execute_query, 3)

      payload =
        if context.sandbox?,
          do: %{"sandbox_query" => "SELECT n FROM data", "query_mode" => "sql"},
          else: %{"query" => "SELECT 1 AS n"}

      assert render_hook(view, "run-query", %{"run" => payload}) =~
               "Not authorized to run this endpoint"

      save_settings(view)

      assert Endpoints.get_endpoint_query(context.endpoint.id).enforced_clickhouse_settings == %{
               "max_execution_time" => 7,
               "timeout_overflow_mode" => "throw"
             }
    end
  end

  test "policy audit rows do not enter or paginate restorable SQL history", context do
    admin = insert(:user, admin: true)

    assert {:ok, endpoint} =
             Endpoints.update_query(
               context.owner,
               context.endpoint,
               %{description: "SQL version"},
               context.owner
             )

    version = Endpoints.get_endpoint_query_version(endpoint.id, 1)

    for limit <- 1..26 do
      assert {:ok, _} =
               Endpoints.configure_enforced_clickhouse_settings(admin, endpoint, %{
                 "max_execution_time" => limit
               })
    end

    {:ok, view, _} =
      context.conn
      |> login_user(context.owner)
      |> live_with_redirect("/endpoints/#{endpoint.id}/versions")

    assert has_element?(view, "#versions-#{version.id}", "current")
    assert view |> element("#versions-#{version.id}") |> render() =~ "SQL version"
    refute has_element?(view, "button[phx-click='load-more']")

    assert view
           |> render()
           |> Floki.parse_document!()
           |> Floki.find("#endpoint-versions button[phx-click='show-version']")
           |> length() == 1
  end

  test "admins can inspect and clear inactive limits without giving ordinary users access",
       context do
    admin = insert(:user, admin: true)
    postgres = insert(:postgres_backend, user: context.owner)

    assert {:ok, limited} =
             Endpoints.configure_enforced_clickhouse_settings(admin, context.endpoint, %{
               "max_execution_time" => 7,
               "priority" => 3,
               "max_threads" => 2
             })

    assert {:ok, dormant} =
             Endpoints.update_query(
               context.owner,
               limited,
               %{backend_id: postgres.id},
               context.owner
             )

    {:ok, owner_view, _} =
      context.conn
      |> login_user(context.owner)
      |> live_with_redirect("/endpoints/#{dormant.id}/edit")

    refute has_element?(owner_view, "#enforced-settings-panel")

    assert render_hook(owner_view, "save-enforced-settings", %{"settings" => %{}}) =~
             "Not authorized to configure ClickHouse settings"

    {:ok, view, _} =
      context.conn |> login_user(admin) |> live_with_redirect("/endpoints/#{dormant.id}/edit")

    assert has_element?(view, "#inactive-enforced-settings", "inactive")
    assert has_element?(view, "#enforced-settings-panel dd", "7")
    refute has_element?(view, "#enforced-settings-form input[type='number']")

    view |> element("#enforced-settings-form") |> render_submit()

    refute has_element?(view, "#enforced-settings-panel")
    assert Endpoints.get_endpoint_query(dormant.id).enforced_clickhouse_settings == %{}
  end

  test "fractional execution times round-trip through the resource form", context do
    admin = insert(:user, admin: true)

    assert {:ok, endpoint} =
             Endpoints.configure_enforced_clickhouse_settings(admin, context.endpoint, %{
               "max_execution_time" => 0.5
             })

    {:ok, view, _} =
      context.conn |> login_user(admin) |> live_with_redirect("/endpoints/#{endpoint.id}/edit")

    assert has_element?(
             view,
             ~s(input[name="settings[max_execution_time]"][min="0"][step="any"][value="0.5"])
           )

    for key <- ~w(max_bytes_to_read max_rows_to_read max_memory_usage) do
      assert has_element?(view, ~s(input[name="settings[#{key}]"][min="1"][step="1"]))
    end

    for {value, expected} <- [{"0.5", 0.5}, {"1.25", 1.25}, {"1.0e-6", 1.0e-6}, {"7", 7}] do
      html =
        view
        |> element("#enforced-settings-form")
        |> render_submit(%{
          settings: %{max_execution_time: value, max_memory_usage: "1000000"}
        })

      assert html =~ "Enforced ClickHouse settings saved"

      assert Endpoints.get_endpoint_query(endpoint.id).enforced_clickhouse_settings == %{
               "max_execution_time" => expected,
               "max_memory_usage" => 1_000_000,
               "timeout_overflow_mode" => "throw"
             }
    end
  end

  test "resource saves preserve current scheduling settings outside the stale form", context do
    admin = insert(:user, admin: true)

    assert {:ok, endpoint} =
             Endpoints.configure_enforced_clickhouse_settings(admin, context.endpoint, %{
               "priority" => 3,
               "max_threads" => 2,
               "max_execution_time" => 7
             })

    {:ok, view, _} =
      context.conn |> login_user(admin) |> live_with_redirect("/endpoints/#{endpoint.id}/edit")

    assert {:ok, _} =
             Endpoints.configure_enforced_clickhouse_settings(admin, endpoint, %{
               "priority" => 9,
               "max_threads" => 1,
               "max_execution_time" => 7
             })

    html =
      render_hook(view, "save-enforced-settings", %{
        "settings" => %{
          "max_execution_time" => "7",
          "max_memory_usage" => "1000000",
          "priority" => "1",
          "max_threads" => "99"
        }
      })

    assert html =~ "Enforced ClickHouse settings saved"

    assert Endpoints.get_endpoint_query(endpoint.id).enforced_clickhouse_settings == %{
             "priority" => 9,
             "max_threads" => 1,
             "max_execution_time" => 7,
             "max_memory_usage" => 1_000_000,
             "timeout_overflow_mode" => "throw"
           }
  end

  test "blank resource fields clear limits and overflow modes without clearing scheduling",
       context do
    admin = insert(:user, admin: true)

    assert {:ok, endpoint} =
             Endpoints.configure_enforced_clickhouse_settings(admin, context.endpoint, %{
               "priority" => 3,
               "max_threads" => 2,
               "max_execution_time" => 0.5,
               "max_rows_to_read" => 10,
               "max_bytes_to_read" => 100,
               "max_memory_usage" => 1000
             })

    {:ok, view, _} =
      context.conn |> login_user(admin) |> live_with_redirect("/endpoints/#{endpoint.id}/edit")

    view
    |> element("#enforced-settings-form")
    |> render_submit(%{
      settings: %{
        max_execution_time: "",
        max_rows_to_read: "",
        max_bytes_to_read: "",
        max_memory_usage: ""
      }
    })

    assert Endpoints.get_endpoint_query(endpoint.id).enforced_clickhouse_settings == %{
             "priority" => 3,
             "max_threads" => 2
           }
  end

  test "the resource form rejects fractional counts and invalid execution times", context do
    admin = insert(:user, admin: true)

    {:ok, view, _} =
      context.conn
      |> login_user(admin)
      |> live_with_redirect("/endpoints/#{context.endpoint.id}/edit")

    for {key, value} <- [
          {"max_bytes_to_read", "0.5"},
          {"max_rows_to_read", "1.5"},
          {"max_memory_usage", "2.5"},
          {"max_execution_time", "0"},
          {"max_execution_time", "-0.5"},
          {"max_execution_time", "0.5junk"},
          {"max_execution_time", "1.0e9999"}
        ] do
      html = render_hook(view, "save-enforced-settings", %{"settings" => %{key => value}})
      assert html =~ "Invalid enforced ClickHouse setting"
      assert html =~ key
      assert Endpoints.get_endpoint_query(context.endpoint.id).enforced_clickhouse_settings == %{}
    end
  end

  defp save_settings(view) do
    view
    |> element("#enforced-settings-form")
    |> render_submit(%{settings: %{max_execution_time: "7"}})
  end
end
