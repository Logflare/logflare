defmodule Logflare.EndpointsTest do
  use Logflare.DataCase, async: false

  alias Logflare.Endpoints
  alias Logflare.Endpoints.Query
  alias Logflare.Backends.Adaptor.PostgresAdaptor

  test "list_endpoints_by" do
    %{id: id, name: name} = insert(:endpoint)
    assert [%{id: ^id}] = Endpoints.list_endpoints_by(name: name)
  end

  test "get_endpoint_query/1 retrieves endpoint" do
    %{id: id} = insert(:endpoint)
    assert %Query{id: ^id} = Endpoints.get_endpoint_query(id)
  end

  test "get_by/1" do
    endpoint = insert(:endpoint, name: "some endpoint")
    assert endpoint.id == Endpoints.get_by(name: "some endpoint").id
  end

  test "get_query_by_token/1" do
    %{id: id, token: token} = insert(:endpoint)
    assert %Query{id: ^id} = Endpoints.get_query_by_token(token)
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
             Endpoints.parse_query_string("select @testing as date")
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

  describe "running queries in bigquery backends" do
    setup do
      # mock goth behaviour
      stub(Goth, :fetch, fn _mod -> {:ok, %Goth.Token{token: "auth-token"}} end)
      :ok
    end

    test "run an endpoint query without caching" do
      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"testing" => "123"}])}
      end)

      insert(:plan)
      user = insert(:user)
      insert(:source, user: user, name: "c")
      endpoint = insert(:endpoint, user: user, query: "select current_datetime() as testing")
      assert {:ok, %{rows: [%{"testing" => _}]}} = Endpoints.run_query(endpoint)
    end

    test "run_query_string/3" do
      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"testing" => "123"}])}
      end)

      insert(:plan)
      user = insert(:user)
      insert(:source, user: user, name: "c")
      query_string = "select current_datetime() as testing"

      assert {:ok, %{rows: [%{"testing" => _}]}} =
               Endpoints.run_query_string(user, {:bq_sql, query_string})
    end

    test "run_cached_query/1" do
      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"testing" => "123"}])}
      end)

      insert(:plan)
      user = insert(:user)
      endpoint = insert(:endpoint, user: user, query: "select current_datetime() as testing")
      _pid = start_supervised!({Logflare.Endpoints.Cache, {endpoint, %{}}})
      assert {:ok, %{rows: [%{"testing" => _}]}} = Endpoints.run_cached_query(endpoint)
      # 2nd query should hit local cache
      assert {:ok, %{rows: [%{"testing" => _}]}} = Endpoints.run_cached_query(endpoint)
    end

    for field_changed <- [
          :query,
          :sandboxable,
          :cache_duration_seconds,
          :proactive_requerying_seconds,
          :max_limit
        ] do
      test "update_query/2 will kill all existing caches on field change (#{field_changed})" do
        expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 2, fn _, _, _ ->
          {:ok, TestUtils.gen_bq_response([%{"testing" => "123"}])}
        end)

        insert(:plan)
        user = insert(:user)
        endpoint = insert(:endpoint, user: user, query: "select current_datetime() as testing")
        cache_pid = start_supervised!({Logflare.Endpoints.Cache, {endpoint, %{}}})
        assert {:ok, %{rows: [%{"testing" => _}]}} = Endpoints.run_cached_query(endpoint)

        params =
          case unquote(field_changed) do
            :query -> %{query: "select 'datetime' as date"}
            :sandboxable -> %{sandboxable: true}
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
      insert(:plan)

      cfg = Application.get_env(:logflare, Logflare.Repo)

      url = "postgresql://#{cfg[:username]}:#{cfg[:password]}@#{cfg[:hostname]}/#{cfg[:database]}"

      user = insert(:user)
      source = insert(:source, user: user, name: "c")

      source_backend =
        insert(:source_backend,
          type: :postgres,
          config: %{"url" => url},
          source: source
        )

      PostgresAdaptor.create_repo(source_backend)
      assert :ok = PostgresAdaptor.connected?(source_backend)
      PostgresAdaptor.create_log_events_table(source_backend)

      on_exit(fn ->
        PostgresAdaptor.rollback_migrations(source_backend)
        PostgresAdaptor.drop_migrations_table(source_backend)
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

    _pid = start_supervised!({Logflare.Endpoints.Cache, {endpoint, %{}}})

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
end
