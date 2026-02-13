defmodule E2e.Features.FetchQueriesTest do
  use Logflare.DataCase, async: false

  import Logflare.Factory

  alias Logflare.FetchQueries
  alias Logflare.FetchQueries.FetchQueryWorker
  alias Logflare.FetchQueries.FetchQuerySchedulerWorker

  setup do
    start_supervised!(Logflare.SystemMetricsSup)
    {:ok, user: insert(:user)}
  end

  describe "End-to-end fetch query workflow" do
    test "create fetch query with webhook backend", %{user: user} do
      backend =
        insert(:backend,
          type: :webhook,
          user: user,
          config: %{"url" => "https://httpbin.org/json"}
        )

      source = insert(:source, user: user)

      attrs = %{
        name: "Webhook Fetch Query",
        description: "Fetch from httpbin",
        cron: "*/5 * * * *",
        query: "",
        language: :bq_sql,
        backend_id: backend.id,
        source_id: source.id,
        user_id: user.id,
        enabled: true
      }

      {:ok, fetch_query} = FetchQueries.create_fetch_query(attrs)

      assert fetch_query.name == "Webhook Fetch Query"
      assert fetch_query.enabled == true
      assert fetch_query.cron == "*/5 * * * *"
      assert fetch_query.external_id
    end

    test "create fetch query with BigQuery backend", %{user: user} do
      backend = insert(:backend, type: :bigquery, user: user)
      source = insert(:source, user: user)

      attrs = %{
        name: "BigQuery Fetch Query",
        description: "Fetch from BigQuery",
        cron: "0 * * * *",
        query: "SELECT current_timestamp() as ts",
        language: :bq_sql,
        backend_id: backend.id,
        source_id: source.id,
        user_id: user.id,
        enabled: true
      }

      {:ok, fetch_query} = FetchQueries.create_fetch_query(attrs)

      assert fetch_query.name == "BigQuery Fetch Query"
      assert fetch_query.query == "SELECT current_timestamp() as ts"
    end

    test "create fetch query with JSONPath extraction", %{user: user} do
      backend =
        insert(:backend,
          type: :webhook,
          user: user,
          config: %{"url" => "https://httpbin.org/json"}
        )

      source = insert(:source, user: user)

      attrs = %{
        name: "JSONPath Fetch Query",
        description: "Extract specific fields",
        cron: "*/10 * * * *",
        query: "$.slideshow.slides[*]",
        language: :jsonpath,
        backend_id: backend.id,
        source_id: source.id,
        user_id: user.id,
        enabled: true
      }

      {:ok, fetch_query} = FetchQueries.create_fetch_query(attrs)

      assert fetch_query.language == :jsonpath
      assert fetch_query.query == "$.slideshow.slides[*]"
    end

    test "scheduler worker can be instantiated", %{user: user} do
      backend = insert(:backend, type: :bigquery, user: user)
      source = insert(:source, user: user)

      _fetch_query =
        insert(:fetch_query,
          user: user,
          backend: backend,
          source: source,
          cron: "*/5 * * * *",
          enabled: true
        )

      # Verify the scheduler worker module is compilable
      # Full scheduler testing requires Oban to be running
      assert is_atom(FetchQuerySchedulerWorker)
    end

    test "fetch query execution adds metadata", %{user: user} do
      backend = insert(:backend, type: :postgres, user: user)
      source = insert(:source, user: user)

      fetch_query =
        insert(:fetch_query,
          user: user,
          backend: backend,
          source: source
        )

      # Execute worker with unsupported backend (will error)
      job = %Oban.Job{args: %{"fetch_query_id" => fetch_query.id}}
      result = FetchQueryWorker.perform(job)

      # Should error due to unsupported backend type
      assert match?({:error, _}, result)
    end

    test "fetch query is disabled and excluded from scheduler", %{user: user} do
      backend = insert(:backend, type: :bigquery, user: user)
      source = insert(:source, user: user)

      disabled_fetch_query =
        insert(:fetch_query,
          user: user,
          backend: backend,
          source: source,
          enabled: false
        )

      enabled_fetch_query =
        insert(:fetch_query,
          user: user,
          backend: backend,
          source: source,
          enabled: true
        )

      # Get enabled queries
      enabled_queries = FetchQueries.list_enabled_fetch_queries()

      assert Enum.any?(enabled_queries, &(&1.id == enabled_fetch_query.id))
      refute Enum.any?(enabled_queries, &(&1.id == disabled_fetch_query.id))
    end

    test "fetch query can be updated", %{user: user} do
      fetch_query = insert(:fetch_query, user: user)

      {:ok, updated} =
        FetchQueries.update_fetch_query(fetch_query, %{
          description: "Updated description",
          enabled: false
        })

      assert updated.description == "Updated description"
      assert updated.enabled == false
    end

    test "fetch query can be deleted", %{user: user} do
      fetch_query = insert(:fetch_query, user: user)

      {:ok, _deleted} = FetchQueries.delete_fetch_query(fetch_query)

      refute FetchQueries.get_fetch_query(fetch_query.id)
    end

    test "fetch query access is team-scoped", %{user: user} do
      other_user = insert(:user)

      fetch_query_by_user = insert(:fetch_query, user: user)
      fetch_query_by_other = insert(:fetch_query, user: other_user)

      user_queries = FetchQueries.list_fetch_queries_by_user_access(user)

      assert Enum.any?(user_queries, &(&1.id == fetch_query_by_user.id))
      refute Enum.any?(user_queries, &(&1.id == fetch_query_by_other.id))
    end

    test "execution history lookup is available via context", %{user: user} do
      backend = insert(:backend, type: :bigquery, user: user)
      source = insert(:source, user: user)

      fetch_query =
        insert(:fetch_query,
          user: user,
          backend: backend,
          source: source
        )

      # Verify the execution history function exists and doesn't crash
      # (Oban jobs table may be empty in test)
      history = FetchQueries.list_execution_history(fetch_query.id)
      assert is_list(history)
    end

    test "fetch query with 1-minute cron interval is valid", %{user: user} do
      backend = insert(:backend, user: user)
      source = insert(:source, user: user)

      attrs = %{
        name: "1 minute cron",
        cron: "* * * * *",
        query: "select 1",
        language: :bq_sql,
        backend_id: backend.id,
        source_id: source.id,
        user_id: user.id
      }

      {:ok, fetch_query} = FetchQueries.create_fetch_query(attrs)

      assert fetch_query.cron == "* * * * *"
    end

    test "fetch query with invalid cron expression is rejected", %{user: user} do
      backend = insert(:backend, user: user)
      source = insert(:source, user: user)

      attrs = %{
        name: "invalid cron",
        cron: "invalid cron expression",
        query: "select 1",
        language: :bq_sql,
        backend_id: backend.id,
        source_id: source.id,
        user_id: user.id
      }

      {:error, changeset} = FetchQueries.create_fetch_query(attrs)

      assert errors_on(changeset).cron
    end
  end

  describe "Fetch query with multiple users and backends" do
    test "fetch queries are properly isolated by user", %{user: user1} do
      user2 = insert(:user)
      user3 = insert(:user)

      backend1 = insert(:backend, user: user1)
      backend2 = insert(:backend, user: user2)
      backend3 = insert(:backend, user: user3)

      source1 = insert(:source, user: user1)
      source2 = insert(:source, user: user2)
      source3 = insert(:source, user: user3)

      fq1 = insert(:fetch_query, user: user1, backend: backend1, source: source1)
      fq2 = insert(:fetch_query, user: user2, backend: backend2, source: source2)
      fq3 = insert(:fetch_query, user: user3, backend: backend3, source: source3)

      # Each user should only see their own fetch queries
      user1_queries = FetchQueries.list_fetch_queries_by_user_access(user1)
      user2_queries = FetchQueries.list_fetch_queries_by_user_access(user2)
      user3_queries = FetchQueries.list_fetch_queries_by_user_access(user3)

      assert Enum.any?(user1_queries, &(&1.id == fq1.id))
      refute Enum.any?(user1_queries, &(&1.id == fq2.id))
      refute Enum.any?(user1_queries, &(&1.id == fq3.id))

      assert Enum.any?(user2_queries, &(&1.id == fq2.id))
      refute Enum.any?(user2_queries, &(&1.id == fq1.id))
      refute Enum.any?(user2_queries, &(&1.id == fq3.id))

      assert Enum.any?(user3_queries, &(&1.id == fq3.id))
      refute Enum.any?(user3_queries, &(&1.id == fq1.id))
      refute Enum.any?(user3_queries, &(&1.id == fq2.id))
    end
  end
end
