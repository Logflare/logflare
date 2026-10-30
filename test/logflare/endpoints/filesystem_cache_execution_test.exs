defmodule Logflare.Endpoints.FilesystemCacheExecutionTest do
  use Logflare.DataCase

  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Endpoints
  alias Logflare.Endpoints.EndpointQuery
  alias Logflare.Endpoints.ResultsCache

  @setting "read_from_filesystem_cache_if_exists_otherwise_bypass_cache"
  @policy %{
    "start_param" => "from",
    "end_param" => "to",
    "timestamp_format" => "unix_seconds",
    "bypass_fill_above_seconds" => 3600
  }
  @params %{"from" => "0", "to" => "7200"}
  @query "SELECT @from AS start_time, @to AS end_time"

  setup do
    insert(:plan)
    owner = insert(:user, admin: false)
    backend = insert(:backend, user: owner, type: :clickhouse)
    endpoint = insert(:endpoint, user: owner, backend: backend, language: :ch_sql, query: @query)
    %{owner: owner, endpoint: endpoint, backend: backend}
  end

  test "normal owner and team edits can configure and disable the policy", context do
    team = insert(:team, user: context.owner)
    member = insert(:team_user, team: team)

    for actor <- [context.owner, member] do
      assert {:ok, endpoint} =
               Endpoints.update_query(
                 actor,
                 context.endpoint,
                 %{filesystem_cache_policy: @policy},
                 actor
               )

      assert endpoint.filesystem_cache_policy == @policy
      assert Jason.decode!(Jason.encode!(endpoint))["filesystem_cache_policy"] == @policy

      assert {:ok, cleared} =
               Endpoints.update_query(actor, endpoint, %{filesystem_cache_policy: nil}, actor)

      assert cleared.filesystem_cache_policy == nil
    end
  end

  test "an unrelated user cannot change the policy", context do
    attacker = insert(:user)

    assert {:error, changeset} =
             Endpoints.update_query(
               attacker,
               context.endpoint,
               %{filesystem_cache_policy: @policy},
               attacker
             )

    assert errors_on(changeset).base == ["Endpoint not found"]
    assert Repo.get!(EndpointQuery, context.endpoint.id).filesystem_cache_policy == nil
  end

  test "configuration validates endpoint type, declared names, query edits and thresholds",
       context do
    for attrs <- [
          %{filesystem_cache_policy: Map.put(@policy, "start_param", "missing")},
          %{filesystem_cache_policy: Map.put(@policy, "bypass_fill_above_seconds", 0)},
          %{filesystem_cache_policy: @policy, language: :bq_sql}
        ] do
      changeset = Endpoints.change_query(context.endpoint, attrs)
      refute changeset.valid?
      assert errors_on(changeset).filesystem_cache_policy
    end

    configured = %{context.endpoint | filesystem_cache_policy: @policy}
    changeset = Endpoints.change_query(configured, %{query: "SELECT 1 AS n"})
    refute changeset.valid?
    assert errors_on(changeset).filesystem_cache_policy
  end

  test "policy follows SQL versions and restoring old snapshots clears it", context do
    assert {:ok, first} =
             Endpoints.update_query(
               context.owner,
               context.endpoint,
               %{description: "without policy"},
               context.owner
             )

    assert {:ok, second} =
             Endpoints.update_query(
               context.owner,
               first,
               %{filesystem_cache_policy: @policy},
               context.owner
             )

    assert {:ok, version1} = Endpoints.get_endpoint_query_at_version(second, 1)
    assert version1.filesystem_cache_policy == nil
    assert {:ok, version2} = Endpoints.get_endpoint_query_at_version(second, 2)
    assert version2.filesystem_cache_policy == @policy

    assert {:ok, restored, 1} =
             Endpoints.restore_query_version(context.owner, second, 1, context.owner)

    assert restored.filesystem_cache_policy == nil

    assert EndpointQuery.version_snapshot_attrs(%{"query" => @query}).filesystem_cache_policy ==
             nil
  end

  test "execution and transformed preview apply the same final outer setting", context do
    endpoint = %{context.endpoint | filesystem_cache_policy: @policy, max_limit: 10}

    expect(ClickHouseAdaptor, :execute_query, fn backend, {sql, _, params, _endpoint}, opts ->
      assert params == @params
      assert opts[:filesystem_cache_settings] == %{@setting => 1}
      assert {:ok, prepared} = ClickHouseAdaptor.prepare_query(backend, sql, opts)
      assert prepared =~ "#{@setting} = 1"
      {:ok, QueryResult.new([%{"ok" => 1}])}
    end)

    assert {:ok, %{rows: [%{"ok" => 1}]}} = Endpoints.run_query(endpoint, @params)
    assert {:ok, preview} = Endpoints.get_transformed_query(endpoint, @params)
    assert preview =~ "#{@setting} = 1"

    assert {:ok, preview} =
             Endpoints.get_transformed_query(endpoint, %{"from" => "0", "to" => "3600"})

    refute preview =~ @setting
  end

  test "sandbox consumers receive one setting on the final outer query", context do
    endpoint = %{
      context.endpoint
      | filesystem_cache_policy: @policy,
        sandboxable: true,
        query:
          "WITH data AS (SELECT @from AS start_time, @to AS end_time) SELECT start_time, end_time FROM data"
    }

    params = Map.put(@params, "sql", "SELECT start_time FROM data")

    expect(ClickHouseAdaptor, :execute_query, fn backend, {sql, _, _, _}, opts ->
      assert {:ok, prepared} = ClickHouseAdaptor.prepare_query(backend, sql, opts)
      assert prepared =~ "WITH data AS"
      assert prepared =~ "SELECT start_time FROM data"
      assert String.ends_with?(prepared, "#{@setting} = 1")
      assert length(Regex.scan(~r/SETTINGS/, prepared)) == 1
      {:ok, QueryResult.new([%{"start_time" => "0"}])}
    end)

    assert {:ok, %{rows: [%{"start_time" => "0"}]}} = Endpoints.run_query(endpoint, params)
    assert {:ok, preview} = Endpoints.get_transformed_query(endpoint, params)
    assert String.ends_with?(preview, "#{@setting} = 1")
  end

  test "malformed request windows fail before backend execution", context do
    reject(ClickHouseAdaptor, :execute_query, 3)
    endpoint = %{context.endpoint | filesystem_cache_policy: @policy}
    assert {:error, message} = Endpoints.run_query(endpoint, %{"from" => "wrong", "to" => "7200"})
    assert message =~ "Filesystem cache policy"
  end

  test "fixed class and endpoint settings override the derived cache hint", context do
    backend = %{
      context.backend
      | config: %{query_class_settings: %{"default" => %{"priority" => 5, @setting => 0}}}
    }

    opts = [query_type: :endpoint, filesystem_cache_settings: %{@setting => 1}]

    assert {:ok, sql} = ClickHouseAdaptor.prepare_query(backend, "SELECT 1", opts)
    assert sql =~ "#{@setting} = 0"
    assert sql =~ "priority = 5"

    opts = Keyword.put(opts, :enforced_clickhouse_settings, %{@setting => 1, "max_threads" => 2})
    assert {:ok, sql} = ClickHouseAdaptor.prepare_query(backend, "SELECT 1", opts)
    assert sql =~ "#{@setting} = 1"
    assert sql =~ "max_threads = 2"

    assert {:error, _} =
             ClickHouseAdaptor.prepare_query(
               backend,
               "SELECT * FROM (SELECT 1 SETTINGS #{@setting} = 0)",
               opts
             )
  end

  test "cached refreshes retain version policy and edits invalidate current caches", context do
    assert {:ok, endpoint} =
             Endpoints.update_query(
               context.owner,
               context.endpoint,
               %{filesystem_cache_policy: @policy},
               context.owner
             )

    parent = self()

    stub(ClickHouseAdaptor, :execute_query, fn backend, {sql, _, _, _}, opts ->
      assert {:ok, prepared} = ClickHouseAdaptor.prepare_query(backend, sql, opts)
      assert prepared =~ "#{@setting} = 1"
      send(parent, :cache_query)
      {:ok, QueryResult.new([%{"ok" => 1}])}
    end)

    cache = start_supervised!({ResultsCache, {endpoint, @params, []}})
    assert {:ok, _} = ResultsCache.query(cache)
    assert_receive :cache_query
    send(cache, :refresh)
    assert_receive :cache_query, 5000
    assert {:ok, _} = ResultsCache.query(cache)

    assert {:ok, _updated} =
             Endpoints.update_query(
               context.owner,
               endpoint,
               %{filesystem_cache_policy: nil},
               context.owner
             )

    assert Endpoints.Resolver.list_caches(endpoint) == []
  end

  test "historical result-cache refreshes do not inherit newer time parameter mappings",
       context do
    assert {:ok, first} =
             Endpoints.update_query(
               context.owner,
               context.endpoint,
               %{filesystem_cache_policy: @policy},
               context.owner
             )

    assert {:ok, current} =
             Endpoints.update_query(
               context.owner,
               first,
               %{filesystem_cache_policy: nil},
               context.owner
             )

    assert {:ok, historical} = Endpoints.get_endpoint_query_at_version(current, 1)

    expect(ClickHouseAdaptor, :execute_query, fn _backend, {_sql, _, _, query}, opts ->
      assert query.filesystem_cache_policy == @policy
      assert opts[:filesystem_cache_settings] == %{@setting => 1}
      {:ok, QueryResult.new([%{"ok" => 1}])}
    end)

    cache = start_supervised!({ResultsCache, {historical, @params, []}})
    assert {:ok, _} = ResultsCache.query(cache)
  end
end
