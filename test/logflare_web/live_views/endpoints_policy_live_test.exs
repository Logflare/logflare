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

  test "admins retain their own privileges inside a non-admin customer's team", context do
    admin = insert(:user, admin: true)
    member = insert(:team_user, team: context.team, email: admin.email)

    {:ok, view, _} =
      context.conn
      |> login_user(context.owner, member)
      |> live_with_redirect("/endpoints/#{context.endpoint.id}/edit?t=#{context.team.id}",
        bypass_team_param: true
      )

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
               "max_execution_time" => 7
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

  defp save_settings(view) do
    view
    |> element("#enforced-settings-form")
    |> render_submit(%{settings: %{max_execution_time: "7"}})
  end
end
