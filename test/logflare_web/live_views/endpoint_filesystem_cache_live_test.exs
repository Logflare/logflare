defmodule LogflareWeb.EndpointFilesystemCacheLiveTest do
  use LogflareWeb.ConnCase

  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Endpoints
  alias Logflare.Endpoints.FilesystemCachePolicy
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
    assert has_element?(view, "input[name='run[params][from]'][phx-debounce='300']")

    view
    |> element("#endpoint")
    |> render_change(%{endpoint: %{filesystem_cache_policy: %{"enabled" => "true"}}})

    assert has_element?(view, "#endpoint_filesystem_cache_policy_window_threshold[value='24']")

    assert has_element?(
             view,
             "#endpoint_filesystem_cache_policy_window_unit option[value='hours'][selected]"
           )

    assert has_element?(
             view,
             "#endpoint_filesystem_cache_policy_timestamp_format option[value='iso8601'][selected]"
           )

    view
    |> element("#endpoint")
    |> render_change(%{endpoint: %{filesystem_cache_policy: @form_policy}})

    assert has_element?(
             view,
             "#endpoint_filesystem_cache_policy_start_param option[value='from']"
           )

    assert has_element?(view, "#endpoint_filesystem_cache_policy_end_param option[value='to']")
    assert has_element?(view, "#endpoint_filesystem_cache_policy_window_threshold[value='1']")

    assert has_element?(
             view,
             "#endpoint_filesystem_cache_policy_window_unit option[value='hours'][selected]"
           )

    view
    |> element("#endpoint")
    |> render_submit(%{endpoint: %{filesystem_cache_policy: @form_policy}})

    saved = Repo.reload!(context.endpoint)
    assert saved.filesystem_cache_policy["bypass_fill_above_seconds"] == 3600

    {:ok, view, _} = live_with_redirect(context.conn, ~p"/endpoints/#{saved.id}/edit")
    assert has_element?(view, "#endpoint_filesystem_cache_policy_window_threshold[value='3600']")

    assert has_element?(
             view,
             "#endpoint_filesystem_cache_policy_window_unit option[value='seconds'][selected]"
           )

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
    assert has_element?(view, "#endpoint_filesystem_cache_policy_window_threshold[value='0']")

    assert has_element?(
             view,
             "#endpoint_filesystem_cache_policy_window_unit option[value='hours'][selected]"
           )

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

  test "blank boundaries in the test form leave the hint unspecified", context do
    {:ok, view, _} = live_with_redirect(context.conn, ~p"/endpoints/#{context.endpoint.id}/edit")

    view
    |> element("#endpoint")
    |> render_change(%{endpoint: %{filesystem_cache_policy: @form_policy}})

    for params <- [%{"from" => "", "to" => "7200"}, %{"from" => "0", "to" => ""}] do
      view
      |> element("form[phx-submit='run-query']")
      |> render_change(%{run: %{params: params}})

      assert has_element?(view, "#filesystem-cache-preview", "No derived cache hint")

      expect(ClickHouseAdaptor, :execute_query, fn _backend, {_sql, _, actual_params, _}, opts ->
        assert actual_params == params
        assert opts[:filesystem_cache_settings] == %{}
        {:ok, QueryResult.new([%{"ok" => 1}])}
      end)

      view
      |> element("form[phx-submit='run-query']")
      |> render_submit(%{run: %{query: @query, params: params}})

      assert render(view) =~ "Ran query successfully"
    end
  end

  test "switching to a non-ClickHouse backend clears the policy and permits saving", context do
    assert {:ok, policy} = FilesystemCachePolicy.normalize(@form_policy)

    for type <- [:bigquery, :postgres] do
      backend = insert(:backend, user: context.owner, type: type)

      endpoint =
        insert(:endpoint,
          user: context.owner,
          backend: context.endpoint.backend,
          language: :ch_sql,
          query: @query,
          filesystem_cache_policy: policy
        )

      {:ok, view, _} = live_with_redirect(context.conn, ~p"/endpoints/#{endpoint.id}/edit")

      view
      |> element("#endpoint")
      |> render_change(%{endpoint: %{backend_id: backend.id}})

      refute has_element?(view, "#filesystem-cache-policy")

      view
      |> element("#endpoint")
      |> render_submit(%{endpoint: %{backend_id: backend.id}})

      saved = Repo.reload!(endpoint)
      assert saved.backend_id == backend.id
      assert saved.filesystem_cache_policy == nil
      assert saved.language == if(type == :postgres, do: :pg_sql, else: :bq_sql)
      assert render(view) =~ "Successfully updated endpoint"
    end
  end

  test "policy errors remain actionable in the editor and sandbox test panel", context do
    assert {:ok, endpoint} =
             Endpoints.update_query(
               context.owner,
               context.endpoint,
               %{
                 filesystem_cache_policy: @form_policy,
                 sandboxable: true,
                 query:
                   "WITH data AS (SELECT @from AS start_time, @to AS end_time) SELECT start_time, end_time FROM data"
               },
               context.owner
             )

    reject(ClickHouseAdaptor, :execute_query, 3)
    {:ok, view, _} = live_with_redirect(context.conn, ~p"/endpoints/#{endpoint.id}/edit")

    view
    |> element("form[phx-submit='run-query']")
    |> render_submit(%{run: %{params: %{"from" => "bad", "to" => "7200"}}})

    assert render(view) =~ "valid start/end timestamps"

    {:ok, view, _} = live_with_redirect(context.conn, ~p"/endpoints/#{endpoint.id}")

    view
    |> element("#endpoint-test-form")
    |> render_submit(%{
      run: %{
        params: %{"from" => "bad", "to" => "7200"},
        sandbox_query: "SELECT start_time FROM data",
        query_mode: "sql"
      }
    })

    assert render(view) =~ "valid start/end timestamps"
    refute render(view) =~ "Please verify your query syntax."
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
    assert has_element?(view, "#filesystem-cache-preview", "No derived cache hint")
    assert has_element?(view, "input[name='run[params][from]'][phx-debounce='300']")

    view
    |> element("#endpoint-test-form")
    |> render_change(%{run: %{params: %{"from" => "0", "to" => "3600"}}})

    assert has_element?(view, "#filesystem-cache-preview", "Normal filesystem-cache behavior")
    assert has_element?(view, "#filesystem-cache-preview", "Derived behavior")
  end
end
