defmodule Logflare.EndpointsTest do
  use Logflare.DataCase

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Endpoints
  alias Logflare.Endpoints.Query

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

  test "update_query/2 " do
    user = insert(:user)
    insert(:source, user: user, name: "my_table")
    endpoint = insert(:endpoint, user: user, query: "select current_datetime() as date")
    sql = "select a from my_table"
    assert {:ok, %{query: ^sql}} = Endpoints.update_query(endpoint, %{query: sql})

    # does not allow updating of query with unknown sources
    assert {:error, %Ecto.Changeset{}} =
             Endpoints.update_query(endpoint, %{query: "select b from unknown"})
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
             Endpoints.create_query(user, %{
               name: "fully-qualified",
               query: "select @test from #{source.name}",
               language: :bq_sql
             })

    assert stored_sql =~ "mysource"
    assert mapping["mysource"] == Atom.to_string(source.token)
  end

  test "create endpoint with fully-qualified names " do
    user = insert(:user, bigquery_project_id: "myproject")

    assert {:ok, %_{query: stored_sql, source_mapping: mapping}} =
             Endpoints.create_query(user, %{
               name: "fully-qualified",
               query: "select @test from `myproject.mydataset.mytable`",
               language: :bq_sql
             })

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
             Endpoints.create_query(user, %{
               name: "fully-qualified.name",
               query: "select testing from `my.date`",
               language: :bq_sql
             })

    assert mapping == %{}
    assert stored_sql =~ "my.date"
  end

  describe "language inference from backend" do
    test "postgres backend maps to `pg_sql` language" do
      user = insert(:user)
      backend = insert(:backend, user: user, type: :postgres)

      assert {:ok, endpoint} =
               Endpoints.create_query(user, %{
                 name: "postgres-endpoint",
                 query: "select current_date as date",
                 backend_id: backend.id
                 # Note: no language specified - should be inferred
               })

      assert endpoint.language == :pg_sql
      assert endpoint.backend_id == backend.id
    end

    test "bigquery backend maps to `bq_sql` language" do
      user = insert(:user)
      backend = insert(:backend, user: user, type: :bigquery)

      assert {:ok, endpoint} =
               Endpoints.create_query(user, %{
                 name: "bigquery-endpoint",
                 query: "select current_date() as date",
                 backend_id: backend.id
               })

      assert endpoint.language == :bq_sql
      assert endpoint.backend_id == backend.id
    end

    test "backend does not overwrite explicit language definition" do
      user = insert(:user)
      backend = insert(:backend, user: user, type: :bigquery)

      assert {:ok, endpoint} =
               Endpoints.create_query(user, %{
                 name: "bigquery-endpoint-lql-test",
                 query: "select current_date() as date",
                 backend_id: backend.id,
                 language: :pg_sql
               })

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
        {:ok, [%{"testing" => "123"}]}
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
        {:ok, [%{"testing" => "123"}]}
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
      test "update_query/2 will kill all existing caches on field change (#{field_changed})" do
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

        assert {:ok, updated} = Endpoints.update_query(endpoint, params)
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

  describe "endpoint-level bigquery reservations" do
    test "run_query/1 uses single endpoint reservation" do
      pid = self()

      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, opts ->
        send(pid, {:reservation, opts[:body].reservation})
        {:ok, TestUtils.gen_bq_response([%{"testing" => "123"}])}
      end)

      user = insert(:user)
      reservation = "projects/123/locations/us/reservations/my-endpoint-res"

      endpoint =
        insert(:endpoint,
          user: user,
          query: "select current_datetime() as testing",
          bigquery_reservations: reservation
        )

      assert {:ok, %{rows: [%{"testing" => _}]}} = Endpoints.run_query(endpoint)

      assert_receive {:reservation, ^reservation}
    end

    test "run_query/1 selects from multiple endpoint reservations" do
      pid = self()

      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, opts ->
        send(pid, {:reservation, opts[:body].reservation})
        {:ok, TestUtils.gen_bq_response([%{"testing" => "123"}])}
      end)

      user = insert(:user)

      reservations =
        "projects/123/locations/us/reservations/res1\nprojects/123/locations/us/reservations/res2"

      endpoint =
        insert(:endpoint,
          user: user,
          query: "select current_datetime() as testing",
          bigquery_reservations: reservations
        )

      assert {:ok, %{rows: [%{"testing" => _}]}} = Endpoints.run_query(endpoint)

      # Verify one of the configured reservations is used
      assert_receive {:reservation, res}

      assert res in [
               "projects/123/locations/us/reservations/res1",
               "projects/123/locations/us/reservations/res2"
             ]
    end

    test "run_query/1 ignores empty lines in reservations" do
      pid = self()

      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, opts ->
        send(pid, {:reservation, opts[:body].reservation})
        {:ok, TestUtils.gen_bq_response([%{"testing" => "123"}])}
      end)

      user = insert(:user)

      # Reservations with empty lines and whitespace
      reservations = """
      projects/123/locations/us/reservations/valid-res


      """

      endpoint =
        insert(:endpoint,
          user: user,
          query: "select current_datetime() as testing",
          bigquery_reservations: reservations
        )

      assert {:ok, %{rows: [%{"testing" => _}]}} = Endpoints.run_query(endpoint)

      # Should use the only valid reservation
      assert_receive {:reservation, "projects/123/locations/us/reservations/valid-res"}
    end

    test "run_query/1 does not set reservation when bigquery_reservations is nil" do
      pid = self()

      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, opts ->
        send(pid, {:reservation, opts[:body].reservation})
        {:ok, TestUtils.gen_bq_response([%{"testing" => "123"}])}
      end)

      user = insert(:user)

      endpoint =
        insert(:endpoint,
          user: user,
          query: "select current_datetime() as testing",
          bigquery_reservations: nil
        )

      assert {:ok, %{rows: [%{"testing" => _}]}} = Endpoints.run_query(endpoint)

      assert_receive {:reservation, nil}
    end

    test "run_query/1 does not set reservation when bigquery_reservations is empty string" do
      pid = self()

      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, opts ->
        send(pid, {:reservation, opts[:body].reservation})
        {:ok, TestUtils.gen_bq_response([%{"testing" => "123"}])}
      end)

      user = insert(:user)

      endpoint =
        insert(:endpoint,
          user: user,
          query: "select current_datetime() as testing",
          bigquery_reservations: ""
        )

      assert {:ok, %{rows: [%{"testing" => _}]}} = Endpoints.run_query(endpoint)

      assert_receive {:reservation, nil}
    end

    test "run_query/1 does not set reservation when bigquery_reservations contains only whitespace" do
      pid = self()

      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, opts ->
        send(pid, {:reservation, opts[:body].reservation})
        {:ok, TestUtils.gen_bq_response([%{"testing" => "123"}])}
      end)

      user = insert(:user)

      endpoint =
        insert(:endpoint,
          user: user,
          query: "select current_datetime() as testing",
          bigquery_reservations: "   \n  \n   "
        )

      assert {:ok, %{rows: [%{"testing" => _}]}} = Endpoints.run_query(endpoint)

      assert_receive {:reservation, nil}
    end
  end
end
