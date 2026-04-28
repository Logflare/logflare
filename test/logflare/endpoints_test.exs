defmodule Logflare.EndpointsTest do
  use Logflare.DataCase

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Endpoints
  alias Logflare.Endpoints.Query
  alias PaperTrail.Version

  @endpoint_query_attrs %{
    name: "history-endpoint",
    query: "select current_date() as date",
    language: :bq_sql
  }

  setup do
    insert(:plan)
    :ok
  end

  test "list_endpoints_by" do
    %{id: id, name: name} = insert(:endpoint)
    assert [%{id: ^id}] = Endpoints.list_endpoints_by(name: name)
  end

  test "list_endpoints_by_user_access" do
    user = insert(:user)
    team_user = insert(:team_user, email: user.email)

    %Query{id: endpoint_id} = insert(:endpoint, user: user)
    %Query{id: other_endpoint_id} = insert(:endpoint, user: team_user.team.user)
    %Query{id: forbidden_endpoint_id} = insert(:endpoint, user: build(:user))

    endpoint_ids =
      Endpoints.list_endpoints_by_user_access(user)
      |> Enum.map(& &1.id)

    assert endpoint_id in endpoint_ids
    assert other_endpoint_id in endpoint_ids
    refute forbidden_endpoint_id in endpoint_ids
  end

  test "get_endpoint_query_by_user_access/2" do
    owner = insert(:user)
    team_user = insert(:team_user, email: owner.email)
    %Query{id: endpoint_id} = insert(:endpoint, user: owner)
    %Query{id: other_endpoint_id} = insert(:endpoint, user: team_user.team.user)
    %Query{id: forbidden_endpoint_id} = insert(:endpoint, user: build(:user))

    assert %Query{id: ^endpoint_id} =
             Endpoints.get_endpoint_query_by_user_access(owner, endpoint_id)

    assert %Query{id: ^endpoint_id} =
             Endpoints.get_endpoint_query_by_user_access(team_user, endpoint_id)

    assert %Query{id: ^other_endpoint_id} =
             Endpoints.get_endpoint_query_by_user_access(team_user, other_endpoint_id)

    assert nil == Endpoints.get_endpoint_query_by_user_access(owner, forbidden_endpoint_id)
    assert nil == Endpoints.get_endpoint_query_by_user_access(team_user, forbidden_endpoint_id)
  end

  test "get_endpoint_query/1 retrieves endpoint" do
    %{id: id} = insert(:endpoint)
    assert %Query{id: ^id} = Endpoints.get_endpoint_query(id)
  end

  test "get_by/1" do
    endpoint = insert(:endpoint, name: "some endpoint")
    assert endpoint.id == Endpoints.get_by(name: "some endpoint").id
  end

  test "get_mapped_query_by_token/1 transforms renamed source names correctly" do
    user = insert(:user)
    source = insert(:source, user: user, name: "my_table")

    endpoint =
      insert(:endpoint,
        user: user,
        query: "select a from my_table",
        source_mapping: %{"my_table" => source.token}
      )

    # rename the source
    source
    |> Ecto.Changeset.change(name: "new")
    |> Logflare.Repo.update()

    assert %Query{query: mapped_query} = Endpoints.get_mapped_query_by_token(endpoint.token)
    assert String.downcase(mapped_query) == "select a from new"
  end

  describe "version history" do
    setup do
      [user: insert(:user)]
    end

    test "create_query/3 stores originator", %{user: user} do
      team_user = insert(:team_user, team: user.team)

      [user, team_user]
      |> Enum.each(fn originator ->
        assert {:ok, endpoint} =
                 Endpoints.create_query(user, @endpoint_query_attrs, originator)

        assert_endpoint_version(endpoint, 1, originator.email)
      end)
    end

    test "create_query/3 stores API token origin with description" do
      access_token = insert(:access_token, description: "Acme")
      owner = access_token.resource_owner

      assert {:ok, endpoint} =
               Endpoints.create_query(owner, @endpoint_query_attrs, access_token)

      assert_endpoint_version(endpoint, 1, "API: Acme")
    end

    test "create_query/3 stores API token origin as access token id" do
      access_token = insert(:access_token, description: "")
      owner = access_token.resource_owner

      assert {:ok, endpoint} =
               Endpoints.create_query(owner, @endpoint_query_attrs, access_token)

      assert_endpoint_version(endpoint, 1, "API: id #{access_token.id}")
    end

    test "update_query/3 increments the version number and stores the updated snapshot", %{
      user: user
    } do
      insert(:source, user: user, name: "my_table")

      assert {:ok, endpoint} =
               Endpoints.create_query(user, @endpoint_query_attrs, user)

      assert {:ok, updated} =
               Endpoints.update_query(endpoint, %{query: "select a from my_table"}, user)

      assert_endpoint_version(endpoint, 1, user.email)
      assert_endpoint_version(updated, 2, user.email, endpoint.token)
    end

    test "update_query/3 does not create a version row when the update fails", %{user: user} do
      assert {:ok, endpoint} =
               Endpoints.create_query(user, @endpoint_query_attrs, user)

      assert_endpoint_version(endpoint, 1, user.email)

      assert {:error, %Ecto.Changeset{}} =
               Endpoints.update_query(endpoint, %{query: "select b from unknown"}, user)

      assert nil == Endpoints.get_endpoint_query_version_by_version_number(endpoint.id, 2)
    end

    test "delete_query/2 stores the next version after prior history", %{user: user} do
      assert {:ok, endpoint} =
               Endpoints.create_query(user, @endpoint_query_attrs, user)

      assert {:ok, deleted_endpoint} = Endpoints.delete_query(endpoint, user)
      assert_endpoint_version(endpoint, 1, user.email)
      assert_endpoint_version(deleted_endpoint, 2, user.email, endpoint.token)
    end

    test "get_endpoint_query_version_by_version_number/2 returns the requested version", %{
      user: user
    } do
      assert {:ok, endpoint} =
               Endpoints.create_query(
                 user,
                 %{
                   name: "history-endpoint",
                   query: "select current_date() as date",
                   language: :bq_sql
                 },
                 user
               )

      assert_endpoint_version(endpoint, 1, user.email)

      requested_version_number = 1

      assert %Version{meta: %{"version_number" => ^requested_version_number}} =
               Endpoints.get_endpoint_query_version_by_version_number(endpoint.id, 1)

      assert nil == Endpoints.get_endpoint_query_version_by_version_number(endpoint.id, 99)
    end
  end

  test "update_query/3 " do
    user = insert(:user)
    source = insert(:source, user: user, name: "my_table")
    endpoint = insert(:endpoint, user: user, query: "select current_datetime() as date")
    sql = "select a from my_table"
    allow_context_cache_sandbox()
    warm_endpoint_query_validation_caches(source)

    assert {:ok, %{query: ^sql}} = Endpoints.update_query(endpoint, %{query: sql}, user)

    # does not allow updating of query with unknown sources
    assert {:error, %Ecto.Changeset{}} =
             Endpoints.update_query(endpoint, %{query: "select b from unknown"}, user)
  end

  describe "endpoint query history" do
    setup do
      [user: insert(:user), endpoint_params: valid_endpoint_params()]
    end

    test "create_query/3 writes version 1 with User origin", %{
      user: user,
      endpoint_params: endpoint_params
    } do
      assert {:ok, endpoint} = Endpoints.create_query(user, endpoint_params, user)
      origin = user.email

      assert [
               %Version{
                 event: "insert",
                 origin: ^origin,
                 meta: %{"version_number" => 1} = version_meta
               }
             ] = versions_for_endpoint(endpoint)

      assert version_meta["endpoint_snapshot"] ==
               expected_endpoint_snapshot(endpoint, %{"token" => nil})
    end

    test "create_query/3 writes version 1 with TeamUser origin", %{
      user: user,
      endpoint_params: endpoint_params
    } do
      team_user = insert(:team_user, team: insert(:team, user: user))

      assert {:ok, endpoint} = Endpoints.create_query(user, endpoint_params, team_user)
      origin = team_user.email

      assert [
               %Version{
                 event: "insert",
                 origin: ^origin,
                 meta: %{"version_number" => 1} = version_meta
               }
             ] = versions_for_endpoint(endpoint)

      assert version_meta["endpoint_snapshot"] ==
               expected_endpoint_snapshot(endpoint, %{"token" => nil})
    end

    test "create_query/3 writes version 1 with the expected API token description origin", %{
      user: user,
      endpoint_params: endpoint_params
    } do
      access_token = build(:access_token, description: "Acme integration")

      assert {:ok, endpoint} = Endpoints.create_query(user, endpoint_params, access_token)

      assert [
               %Version{
                 event: "insert",
                 origin: "API: Acme integration",
                 meta: %{"version_number" => 1} = version_meta
               }
             ] = versions_for_endpoint(endpoint)

      assert version_meta["endpoint_snapshot"] ==
               expected_endpoint_snapshot(endpoint, %{"token" => nil})
    end

    test "update_query/3 increments the version number and preserves the snapshot contract", %{
      user: user,
      endpoint_params: endpoint_params
    } do
      assert {:ok, endpoint} = Endpoints.create_query(user, endpoint_params, user)

      params = %{
        name: "updated-endpoint",
        query: "select current_datetime() as updated_date",
        description: "updated description",
        sandboxable: true,
        cache_duration_seconds: 120,
        proactive_requerying_seconds: 60,
        max_limit: 250,
        enable_auth: false,
        redact_pii: true,
        enable_dynamic_reservation: true,
        labels: "environment"
      }

      assert {:ok, updated_endpoint} = Endpoints.update_query(endpoint, params, user)
      origin = user.email

      assert [
               %Version{meta: %{"version_number" => 1}},
               %Version{
                 event: "update",
                 origin: ^origin,
                 meta: %{"version_number" => 2} = update_meta
               }
             ] = versions_for_endpoint(endpoint)

      assert update_meta["endpoint_snapshot"] ==
               expected_endpoint_snapshot(updated_endpoint)
    end

    test "update_query/3 rollback path leaves history unchanged when the update fails", %{
      user: user,
      endpoint_params: endpoint_params
    } do
      assert {:ok, endpoint} = Endpoints.create_query(user, endpoint_params, user)

      assert [%Version{id: create_version_id}] = versions_for_endpoint(endpoint)

      assert {:error, %Ecto.Changeset{}} =
               Endpoints.update_query(endpoint, %{query: "select b from unknown"}, user)

      assert [
               %Version{
                 id: ^create_version_id,
                 meta: %{"version_number" => 1}
               }
             ] = versions_for_endpoint(endpoint)
    end

    test "delete_query/2 writes the next version and preserves the final snapshot and origin", %{
      user: user,
      endpoint_params: endpoint_params
    } do
      assert {:ok, endpoint} = Endpoints.create_query(user, endpoint_params, user)

      assert {:ok, updated_endpoint} =
               Endpoints.update_query(endpoint, %{labels: "environment"}, user)

      assert {:ok, deleted_endpoint} = Endpoints.delete_query(updated_endpoint, user)
      origin = user.email

      assert [
               %Version{meta: %{"version_number" => 1}},
               %Version{meta: %{"version_number" => 2}},
               %Version{
                 event: "delete",
                 origin: ^origin,
                 meta: %{"version_number" => 3} = delete_meta
               }
             ] = versions_for_endpoint(endpoint)

      assert delete_meta["endpoint_snapshot"] ==
               expected_endpoint_snapshot(deleted_endpoint)
    end

    test "get_endpoint_query_version_by_version_number/2 resolves integer inputs", %{
      user: user,
      endpoint_params: endpoint_params
    } do
      assert {:ok, endpoint} = Endpoints.create_query(user, endpoint_params, user)

      assert {:ok, _updated_endpoint} =
               Endpoints.update_query(endpoint, %{labels: "environment"}, user)

      assert %Version{id: version_1_id} =
               Endpoints.get_endpoint_query_version_by_version_number(endpoint.id, 1)

      assert %Version{id: version_2_id} =
               Endpoints.get_endpoint_query_version_by_version_number(endpoint.id, 2)

      assert version_1_id != version_2_id
      assert nil == Endpoints.get_endpoint_query_version_by_version_number(endpoint.id, 3)
      assert nil == Endpoints.get_endpoint_query_version_by_version_number(endpoint.id + 1, 1)
    end
  end

  test "parse_query_string/1" do
    assert {:ok, %{parameters: ["testing"]}} =
             Endpoints.parse_query_string(:bq_sql, "select @testing as date", [], [])
  end

  test "parse_query_string/1 for nested queries" do
    nested = insert(:endpoint, name: "nested", query: "select @other as date")

    assert {:ok, %{parameters: ["other"]}} =
             Endpoints.parse_query_string(:bq_sql, "select date from `nested`", [nested], [])
  end

  test "create endpoint with normal source name" do
    user = insert(:user)
    source = insert(:source, user: user, name: "mysource")

    assert {:ok, %_{query: stored_sql, source_mapping: mapping}} =
             Endpoints.create_query(
               user,
               %{
                 name: "fully-qualified",
                 query: "select @test from #{source.name}",
                 language: :bq_sql
               },
               user
             )

    assert stored_sql =~ "mysource"
    assert mapping["mysource"] == Atom.to_string(source.token)
  end

  test "create endpoint with fully-qualified names " do
    user = insert(:user, bigquery_project_id: "myproject")

    assert {:ok, %_{query: stored_sql, source_mapping: mapping}} =
             Endpoints.create_query(
               user,
               %{
                 name: "fully-qualified",
                 query: "select @test from `myproject.mydataset.mytable`",
                 language: :bq_sql
               },
               user
             )

    assert mapping == %{}

    assert stored_sql =~ "myproject"
  end

  test "create an endpoint query with query composition" do
    insert(:plan)
    user = insert(:user)

    insert(:endpoint,
      user: user,
      name: "my.date",
      query: "select current_datetime() as testing"
    )

    assert {:ok, %_{query: stored_sql, source_mapping: mapping}} =
             Endpoints.create_query(
               user,
               %{
                 name: "fully-qualified.name",
                 query: "select testing from `my.date`",
                 language: :bq_sql
               },
               user
             )

    assert mapping == %{}
    assert stored_sql =~ "my.date"
  end

  describe "language inference from backend" do
    test "postgres backend maps to `pg_sql` language" do
      user = insert(:user)
      backend = insert(:backend, user: user, type: :postgres)

      assert {:ok, endpoint} =
               Endpoints.create_query(
                 user,
                 %{
                   name: "postgres-endpoint",
                   query: "select current_date as date",
                   backend_id: backend.id
                   # Note: no language specified - should be inferred
                 },
                 user
               )

      assert endpoint.language == :pg_sql
      assert endpoint.backend_id == backend.id
    end

    test "bigquery backend maps to `bq_sql` language" do
      user = insert(:user)
      backend = insert(:backend, user: user, type: :bigquery)

      assert {:ok, endpoint} =
               Endpoints.create_query(
                 user,
                 %{
                   name: "bigquery-endpoint",
                   query: "select current_date() as date",
                   backend_id: backend.id
                 },
                 user
               )

      assert endpoint.language == :bq_sql
      assert endpoint.backend_id == backend.id
    end

    test "backend does not overwrite explicit language definition" do
      user = insert(:user)
      backend = insert(:backend, user: user, type: :bigquery)

      assert {:ok, endpoint} =
               Endpoints.create_query(
                 user,
                 %{
                   name: "bigquery-endpoint-lql-test",
                   query: "select current_date() as date",
                   backend_id: backend.id,
                   language: :pg_sql
                 },
                 user
               )

      assert endpoint.language == :pg_sql
      assert endpoint.backend_id == backend.id
    end
  end

  describe "running queries in bigquery backends" do
    test "run an endpoint query without caching" do
      pid = self()

      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, opts ->
        send(pid, opts[:body].labels)
        {:ok, TestUtils.gen_bq_response([%{"testing" => "123"}])}
      end)

      user = insert(:user)
      insert(:source, user: user, name: "c")

      %{id: endpoint_id} =
        endpoint = insert(:endpoint, user: user, query: "select current_datetime() as testing")

      assert {:ok, %{rows: [%{"testing" => _}]}} = Endpoints.run_query(endpoint)

      endpoint_id_label_value = Integer.to_string(endpoint_id)

      assert_received %{
        "endpoint_id" => ^endpoint_id_label_value
      }
    end

    test "run an endpoint query with query composition" do
      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, opts ->
        assert opts[:body].query =~ "current_datetime"
        {:ok, TestUtils.gen_bq_response([%{"testing" => "123"}])}
      end)

      user = insert(:user)

      insert(:endpoint,
        user: user,
        name: "my.date",
        query: "select current_datetime() as testing"
      )

      endpoint2 = insert(:endpoint, user: user, query: "select testing from `my.date`")
      assert {:ok, %{rows: [%{"testing" => _}]}} = Endpoints.run_query(endpoint2)
    end

    test "run_query/1 will exec a bq query with parsed labels" do
      pid = self()

      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, opts ->
        send(pid, opts[:body].labels)
        {:ok, TestUtils.gen_bq_response([%{"testing" => "123"}])}
      end)

      user = insert(:user)

      endpoint =
        insert(:endpoint,
          user: user,
          name: "my.date",
          language: :bq_sql,
          query: "select current_datetime() as testing",
          parsed_labels: %{"my_label" => "my_value"}
        )

      assert {:ok, %{rows: [%{"testing" => _}]}} = Endpoints.run_query(endpoint)
      assert_received %{"my_label" => "my_value"}
    end

    test "run_query_string/3" do
      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"testing" => "123"}])}
      end)

      user = insert(:user)
      insert(:source, user: user, name: "c")
      query_string = "select current_datetime() as testing"

      assert {:ok, %{rows: [%{"testing" => _}]}} =
               Endpoints.run_query_string(user, {:bq_sql, query_string})
    end

    test "run_query_string/3 uses specified backend_id when provided" do
      user = insert(:user)
      insert(:source, user: user, name: "c")

      # Create two ClickHouse backends for the same user
      _backend1 = insert(:backend, user: user, type: :clickhouse)
      backend2 = insert(:backend, user: user, type: :clickhouse)

      test_pid = self()

      expect(ClickHouseAdaptor, :execute_query, fn backend, _query, _opts ->
        send(test_pid, {:backend_used, backend.id})
        {:ok, QueryResult.new([%{"testing" => "123"}])}
      end)

      query_string = "SELECT 1 as testing"

      assert {:ok, %{rows: [%{"testing" => "123"}]}} =
               Endpoints.run_query_string(user, {:ch_sql, query_string}, backend_id: backend2.id)

      assert_received {:backend_used, backend_id}
      assert backend_id == backend2.id
    end

    test "run_query_string/3 falls back to first backend of type when backend_id is nil" do
      user = insert(:user)
      insert(:source, user: user, name: "c")

      backend = insert(:backend, user: user, type: :clickhouse)

      test_pid = self()

      expect(ClickHouseAdaptor, :execute_query, fn backend, _query, _opts ->
        send(test_pid, {:backend_used, backend.id})
        {:ok, QueryResult.new([%{"testing" => "123"}])}
      end)

      query_string = "SELECT 1 as testing"

      # When backend_id is not specified, it should fall back to finding by type
      assert {:ok, %{rows: [%{"testing" => "123"}]}} =
               Endpoints.run_query_string(user, {:ch_sql, query_string})

      assert_received {:backend_used, backend_id}
      assert backend_id == backend.id
    end

    test "run_query/1 applies PII redaction based on redact_pii flag" do
      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 2, fn _conn, _proj_id, _opts ->
        {:ok,
         TestUtils.gen_bq_response([
           %{"ip" => "192.168.1.1", "message" => "User 10.0.0.1 logged in"}
         ])}
      end)

      user = insert(:user)

      # test with redact_pii enabled
      endpoint =
        insert(:endpoint,
          user: user,
          query: "select '1.2.3.4' as ip",
          redact_pii: true
        )

      assert {:ok, %{rows: [%{"ip" => "REDACTED", "message" => "User REDACTED logged in"}]}} =
               Endpoints.run_query(endpoint)

      # test with redact_pii disabled
      endpoint =
        insert(:endpoint,
          user: user,
          query: "select 'test' as ip",
          redact_pii: false
        )

      assert {:ok, %{rows: [%{"ip" => "192.168.1.1", "message" => "User 10.0.0.1 logged in"}]}} =
               Endpoints.run_query(endpoint)
    end

    test "run_cached_query/2 applies PII redaction" do
      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"ip" => "192.168.1.1"}])}
      end)

      user = insert(:user)

      endpoint =
        insert(:endpoint,
          user: user,
          query: "select '1.2.3.4' as ip",
          redact_pii: true,
          # Disable caching to ensure fresh query
          cache_duration_seconds: 0
        )

      assert {:ok, %{rows: [%{"ip" => "REDACTED"}]}} =
               Endpoints.run_cached_query(endpoint, %{})
    end

    test "run_cached_query/1" do
      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"testing" => "123"}])}
      end)

      user = insert(:user)

      endpoint =
        insert(:endpoint,
          user: user,
          query: "select current_datetime() as testing",
          cache_duration_seconds: 4
        )

      _pid = start_supervised!({Logflare.Endpoints.ResultsCache, {endpoint, %{}, []}})
      assert {:ok, %{rows: [%{"testing" => _}]}} = Endpoints.run_cached_query(endpoint)
      # 2nd query should hit local cache
      assert {:ok, %{rows: [%{"testing" => _}]}} = Endpoints.run_cached_query(endpoint)
    end

    test "run_cached_query/1 only 1 query run" do
      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"testing" => "123"}])}
      end)

      user = insert(:user)

      endpoint =
        insert(:endpoint,
          user: user,
          query: "select current_datetime() as testing",
          cache_duration_seconds: 1
        )

      _pid = start_supervised!({Logflare.Endpoints.ResultsCache, {endpoint, %{}, []}})
      assert {:ok, %{rows: [%{"testing" => _}]}} = Endpoints.run_cached_query(endpoint)
      # 2nd query should hit local cache
      assert {:ok, %{rows: [%{"testing" => _}]}} = Endpoints.run_cached_query(endpoint)
    end

    test "run_cached_query/1 with cache disabled" do
      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 2, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"testing" => "123"}])}
      end)

      user = insert(:user)

      endpoint =
        insert(:endpoint,
          user: user,
          query: "select current_datetime() as testing",
          cache_duration_seconds: 0
        )

      assert {:ok, %{rows: [%{"testing" => _}]}} = Endpoints.run_cached_query(endpoint)
      # 2nd query should hit local cache
      assert {:ok, %{rows: [%{"testing" => _}]}} = Endpoints.run_cached_query(endpoint)
    end

    for field_changed <- [
          :query,
          :sandboxable,
          :cache_duration_seconds,
          :proactive_requerying_seconds,
          :max_limit,
          :enable_auth,
          :labels
        ] do
      test "update_query/3 will kill all existing caches on field change (#{field_changed})" do
        expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 2, fn _, _, _ ->
          {:ok, TestUtils.gen_bq_response([%{"testing" => "123"}])}
        end)

        user = insert(:user)
        endpoint = insert(:endpoint, user: user, query: "select current_datetime() as testing")
        cache_pid = start_supervised!({Logflare.Endpoints.ResultsCache, {endpoint, %{}, []}})
        assert {:ok, %{rows: [%{"testing" => _}]}} = Endpoints.run_cached_query(endpoint)

        params =
          case unquote(field_changed) do
            :query -> %{query: "select 'datetime' as date"}
            :sandboxable -> %{sandboxable: true}
            :enable_auth -> %{enable_auth: !endpoint.enable_auth}
            :labels -> %{labels: "environment"}
            # integer keys
            key -> Map.new([{key, 123}])
          end

        assert {:ok, updated} = Endpoints.update_query(endpoint, params, user)
        # should kill the cache process
        :timer.sleep(500)
        refute Process.alive?(cache_pid)
        # 2nd query should not hit cache
        assert {:ok, %{rows: [%{"testing" => _}]}} = Endpoints.run_cached_query(updated)
      end
    end
  end

  describe "running queries in postgres backends" do
    setup do
      cfg = Application.get_env(:logflare, Logflare.Repo)

      url = "postgresql://#{cfg[:username]}:#{cfg[:password]}@#{cfg[:hostname]}/#{cfg[:database]}"

      user = insert(:user)
      source = insert(:source, user: user, name: "c")

      backend =
        insert(:backend,
          type: :postgres,
          config: %{url: url},
          sources: [source],
          user: user
        )

      PostgresAdaptor.create_repo(backend)
      PostgresAdaptor.create_events_table({source, backend})

      on_exit(fn ->
        PostgresAdaptor.destroy_instance({source, backend})
      end)

      %{source: source, user: user}
    end

    test "run an endpoint query without caching", %{source: source, user: user} do
      query = "select body from #{source.name}"
      endpoint = insert(:endpoint, user: user, query: query, language: :pg_sql)
      assert {:ok, %{rows: []}} = Endpoints.run_query(endpoint)
    end

    test "run_query_string/3", %{source: source, user: user} do
      query = "select body from #{source.name}"
      assert {:ok, %{rows: []}} = Endpoints.run_query_string(user, {:pg_sql, query})
    end

    test "run_cached_query/1", %{source: source, user: user} do
      query = "select body from #{source.name}"
      endpoint = insert(:endpoint, user: user, query: query, language: :pg_sql)
      assert {:ok, %{rows: []}} = Endpoints.run_cached_query(endpoint)
    end
  end

  test "endpoint metrics - cache count" do
    user = insert(:user)
    endpoint = insert(:endpoint, user: user)
    assert endpoint.metrics == nil

    assert %_{
             metrics: %Query.Metrics{
               cache_count: 0
             }
           } = Endpoints.calculate_endpoint_metrics(endpoint)

    _pid = start_supervised!({Logflare.Endpoints.ResultsCache, {endpoint, %{}, []}})

    assert %_{
             metrics: %Query.Metrics{
               cache_count: 1
             }
           } = Endpoints.calculate_endpoint_metrics(endpoint)

    # accepts lists
    assert [
             %_{
               metrics: %Query.Metrics{
                 cache_count: 1
               }
             }
           ] = Endpoints.calculate_endpoint_metrics([endpoint])
  end

  describe "single tenant mode using bigquery" do
    TestUtils.setup_single_tenant(supabase_mode: true)

    test "run_query/1 will exec a bq query" do
      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, opts ->
        assert opts[:body].query =~ "current_datetime"
        {:ok, TestUtils.gen_bq_response([%{"testing" => "123"}])}
      end)

      user = insert(:user)

      endpoint =
        insert(:endpoint,
          user: user,
          name: "my.date",
          language: :bq_sql,
          query: "select current_datetime() as testing"
        )

      assert {:ok, %{rows: [%{"testing" => _}]}} = Endpoints.run_query(endpoint)
    end
  end

  describe "derive_language_from_backend_id/1" do
    test "returns `:bq_sql` for nil" do
      assert Endpoints.derive_language_from_backend_id(nil) == :bq_sql
    end

    test "returns `:bq_sql` for empty string" do
      assert Endpoints.derive_language_from_backend_id("") == :bq_sql
    end

    test "returns `:bq_sql` for invalid string" do
      assert Endpoints.derive_language_from_backend_id("invalid") == :bq_sql
    end

    test "returns `:bq_sql` for non-existent backend id" do
      assert Endpoints.derive_language_from_backend_id(999_999) == :bq_sql
    end

    test "returns correct language for clickhouse backend" do
      user = insert(:user)
      backend = insert(:backend, user: user, type: :clickhouse)

      assert Endpoints.derive_language_from_backend_id(backend.id) == :ch_sql
      assert Endpoints.derive_language_from_backend_id(to_string(backend.id)) == :ch_sql
    end

    test "returns correct language for postgres backend" do
      user = insert(:user)
      backend = insert(:backend, user: user, type: :postgres)

      # In supabase mode, postgres should return :bq_sql
      assert Endpoints.derive_language_from_backend_id(backend.id) == :pg_sql
    end
  end

  describe "enable_dynamic_reservation" do
    test "changeset/2 casts enable_dynamic_reservation" do
      user = insert(:user)

      changeset =
        Endpoints.change_query(%Query{user: user}, %{
          "name" => "test-endpoint",
          "query" => "select 1",
          "enable_dynamic_reservation" => true
        })

      assert changeset.valid?
      assert Ecto.Changeset.get_change(changeset, :enable_dynamic_reservation) == true
    end

    test "enable_dynamic_reservation defaults to false" do
      endpoint = insert(:endpoint)
      assert endpoint.enable_dynamic_reservation == false
    end

    test "enable_dynamic_reservation can be persisted as true" do
      endpoint = insert(:endpoint, enable_dynamic_reservation: true)
      assert endpoint.enable_dynamic_reservation == true
    end
  end

  defp valid_endpoint_params(attrs \\ %{}) do
    Map.merge(
      %{
        name: "endpoint-#{System.unique_integer([:positive])}",
        query: "select current_datetime() as date",
        description: "endpoint description",
        language: :bq_sql,
        sandboxable: false,
        cache_duration_seconds: 60,
        proactive_requerying_seconds: 30,
        max_limit: 100,
        enable_auth: true,
        redact_pii: false,
        enable_dynamic_reservation: false,
        labels: "env"
      },
      attrs
    )
  end

  defp warm_endpoint_query_validation_caches(source) do
    user = Logflare.Users.Cache.get(source.user_id)

    Logflare.Billing.Cache.get_billing_account_by(user_id: user.id)
    Logflare.Billing.Cache.get_plan_by(name: "Free")
    Logflare.Billing.Cache.get_plan_by_user(user)
  end

  defp versions_for_endpoint(%Query{} = endpoint) do
    endpoint
    |> PaperTrail.get_versions()
    |> Enum.sort_by(& &1.id)
  end

  defp expected_endpoint_snapshot(%Query{} = endpoint, overrides \\ %{}) do
    %{
      "backend_id" => endpoint.backend_id,
      "cache_duration_seconds" => endpoint.cache_duration_seconds,
      "description" => endpoint.description,
      "enable_auth" => endpoint.enable_auth,
      "enable_dynamic_reservation" => endpoint.enable_dynamic_reservation,
      "labels" => endpoint.labels,
      "language" => to_string(endpoint.language),
      "max_limit" => endpoint.max_limit,
      "name" => endpoint.name,
      "proactive_requerying_seconds" => endpoint.proactive_requerying_seconds,
      "query" => endpoint.query,
      "redact_pii" => endpoint.redact_pii,
      "sandboxable" => endpoint.sandboxable,
      "source_mapping" => endpoint.source_mapping,
      "token" => endpoint.token
    }
    |> Map.merge(overrides)
  end

  defp assert_endpoint_version(endpoint, version_number, expected_origin, expected_token \\ nil) do
    assert version =
             Endpoints.get_endpoint_query_version_by_version_number(endpoint.id, version_number)

    assert version.origin == expected_origin
    assert version.meta["version_number"] == version_number

    assert version.meta["endpoint_snapshot"] ==
             expected_endpoint_snapshot(endpoint, %{"token" => expected_token})

    version
  end
end
