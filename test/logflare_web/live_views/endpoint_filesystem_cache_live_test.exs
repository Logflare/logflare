defmodule LogflareWeb.EndpointFilesystemCacheLiveTest do
  use LogflareWeb.ConnCase

  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Endpoints
  alias Logflare.Repo

  @query "SELECT @from AS start_time, @to AS end_time"
  @form_policy %{
    "enabled" => "true",
    "start_param" => "from",
    "end_param" => "to",
    "timestamp_format" => "unix_seconds",
    "window_threshold" => "1",
    "window_unit" => "hours"
  }

  setup %{conn: conn} do
    insert(:plan)
    owner = insert(:user, admin: false, endpoints_beta: true)
    team = insert(:team, user: owner)
    backend = insert(:backend, user: owner, type: :clickhouse)
    endpoint = insert(:endpoint, user: owner, backend: backend, language: :ch_sql, query: @query)
    %{conn: login_user(conn, owner), owner: owner, team: team, endpoint: endpoint}
  end

  test "an ordinary endpoint editor can enable, save and disable the policy", context do
    {:ok, view, _} = live_with_redirect(context.conn, ~p"/endpoints/#{context.endpoint.id}/edit")
    assert has_element?(view, "#filesystem-cache-policy")
    refute has_element?(view, "#enforced-settings-form")

    view
    |> element("#endpoint")
    |> render_change(%{endpoint: %{filesystem_cache_policy: @form_policy}})

    assert has_element?(
             view,
             "#endpoint_filesystem_cache_policy_start_param option[value='from']"
           )

    assert has_element?(view, "#endpoint_filesystem_cache_policy_end_param option[value='to']")

    view
    |> element("#endpoint")
    |> render_submit(%{endpoint: %{filesystem_cache_policy: @form_policy}})

    saved = Repo.reload!(context.endpoint)
    assert saved.filesystem_cache_policy["bypass_fill_above_seconds"] == 3600

    {:ok, view, _} = live_with_redirect(context.conn, ~p"/endpoints/#{saved.id}/edit")

    view
    |> element("#endpoint")
    |> render_submit(%{endpoint: %{filesystem_cache_policy: %{"enabled" => "false"}}})

    assert Repo.reload!(saved).filesystem_cache_policy == nil
  end

  test "authorized team members use the same editor without an extra role", context do
    member = insert(:team_user, team: context.team)

    {:ok, view, _} =
      context.conn
      |> login_user(context.owner, member)
      |> live_with_redirect(~p"/endpoints/#{context.endpoint.id}/edit", bypass_team_param: true)

    assert has_element?(view, "#filesystem-cache-policy")

    view
    |> element("#endpoint")
    |> render_change(%{endpoint: %{filesystem_cache_policy: @form_policy}})

    view
    |> element("#endpoint")
    |> render_submit(%{endpoint: %{filesystem_cache_policy: @form_policy}})

    assert Repo.reload!(context.endpoint).filesystem_cache_policy["start_param"] == "from"
  end

  test "the editor validates parameter mapping and a positive threshold", context do
    {:ok, view, _} = live_with_redirect(context.conn, ~p"/endpoints/#{context.endpoint.id}/edit")

    invalid = Map.put(@form_policy, "window_threshold", "0")

    view
    |> element("#endpoint")
    |> render_change(%{endpoint: %{filesystem_cache_policy: invalid}})

    assert render(view) =~ "positive threshold"

    invalid = Map.put(@form_policy, "start_param", "missing")

    view
    |> element("#endpoint")
    |> render_submit(%{endpoint: %{filesystem_cache_policy: invalid}})

    assert render(view) =~ "both time parameters declared"
    assert Repo.reload!(context.endpoint).filesystem_cache_policy == nil

    view
    |> element("#endpoint")
    |> render_submit(%{endpoint: %{filesystem_cache_policy: "invalid"}})

    assert render(view) =~ "is invalid"
    assert Repo.reload!(context.endpoint).filesystem_cache_policy == nil
  end

  test "unsaved editor policy is reflected in preview and test execution", context do
    {:ok, view, _} = live_with_redirect(context.conn, ~p"/endpoints/#{context.endpoint.id}/edit")

    view
    |> element("#endpoint")
    |> render_change(%{endpoint: %{filesystem_cache_policy: @form_policy}})

    params = %{"from" => "0", "to" => "7200"}

    view
    |> element("form[phx-submit='run-query']")
    |> render_change(%{run: %{params: params}})

    assert has_element?(view, "#filesystem-cache-preview", "Existing cache entries only")
    assert has_element?(view, "#filesystem-cache-preview", "Requested window: 7200 seconds")

    expect(ClickHouseAdaptor, :execute_query, fn backend, {sql, _, _, _}, opts ->
      assert opts[:filesystem_cache_settings] == %{
               "read_from_filesystem_cache_if_exists_otherwise_bypass_cache" => 1
             }

      assert {:ok, prepared} = ClickHouseAdaptor.prepare_query(backend, sql, opts)
      assert prepared =~ "read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 1"
      {:ok, QueryResult.new([%{"ok" => 1}])}
    end)

    view
    |> element("form[phx-submit='run-query']")
    |> render_submit(%{run: %{query: @query, params: params}})

    assert render(view) =~ "Ran query successfully"
    assert Repo.reload!(context.endpoint).filesystem_cache_policy == nil
  end

  test "non-ClickHouse endpoints do not show the policy controls", context do
    endpoint = insert(:endpoint, user: context.owner)
    {:ok, view, _} = live_with_redirect(context.conn, ~p"/endpoints/#{endpoint.id}/edit")
    refute has_element?(view, "#filesystem-cache-policy")
  end

  test "saved endpoint test views show the resolved window", context do
    assert {:ok, endpoint} =
             Endpoints.update_query(
               context.owner,
               context.endpoint,
               %{filesystem_cache_policy: @form_policy},
               context.owner
             )

    {:ok, view, _} = live_with_redirect(context.conn, ~p"/endpoints/#{endpoint.id}")

    view
    |> element("#endpoint-test-form")
    |> render_change(%{run: %{params: %{"from" => "0", "to" => "3600"}}})

    assert has_element?(view, "#filesystem-cache-preview", "Normal filesystem-cache behavior")
  end
end
