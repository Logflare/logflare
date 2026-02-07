defmodule Logflare.FetchQueriesTest do
  use Logflare.DataCase, async: false

  import Logflare.Factory

  alias Logflare.FetchQueries

  setup do
    {:ok, user: insert(:user)}
  end

  describe "create_fetch_query/1" do
    test "creates fetch query with valid attrs", %{user: user} do
      backend = insert(:backend, type: :bigquery, user: user)
      source = insert(:source, user: user)

      attrs = %{
        name: "test fetch",
        cron: "*/5 * * * *",
        query: "select 1",
        language: :bq_sql,
        backend_id: backend.id,
        source_id: source.id,
        user_id: user.id
      }

      {:ok, fetch_query} = FetchQueries.create_fetch_query(attrs)

      assert fetch_query.name == "test fetch"
      assert fetch_query.cron == "*/5 * * * *"
      assert fetch_query.query == "select 1"
      assert fetch_query.backend_id == backend.id
      assert fetch_query.source_id == source.id
    end

    test "validates required fields" do
      {:error, changeset} = FetchQueries.create_fetch_query(%{})

      assert "can't be blank" in errors_on(changeset).name
      assert "can't be blank" in errors_on(changeset).cron
      assert "can't be blank" in errors_on(changeset).source_id
      assert "can't be blank" in errors_on(changeset).user_id
    end

    test "defaults to system default backend when backend_id is blank", %{user: user} do
      source = insert(:source, user: user)
      default_backend = Logflare.Backends.get_default_backend(user)

      attrs = %{
        "name" => "test fetch",
        "cron" => "*/5 * * * *",
        "query" => "select 1",
        "language" => "bq_sql",
        "backend_id" => "",
        "source_id" => source.id,
        "user_id" => user.id
      }

      {:ok, fetch_query} = FetchQueries.create_fetch_query(attrs)

      assert fetch_query.backend_id == default_backend.id
    end

    test "defaults to system default backend when backend_id is nil", %{user: user} do
      source = insert(:source, user: user)
      default_backend = Logflare.Backends.get_default_backend(user)

      attrs = %{
        "name" => "test fetch",
        "cron" => "*/5 * * * *",
        "query" => "select 1",
        "language" => "bq_sql",
        "source_id" => source.id,
        "user_id" => user.id
      }

      {:ok, fetch_query} = FetchQueries.create_fetch_query(attrs)

      assert fetch_query.backend_id == default_backend.id
    end

    test "validates cron expression", %{user: user} do
      backend = insert(:backend, user: user)
      source = insert(:source, user: user)

      attrs = %{
        name: "test",
        cron: "invalid cron",
        query: "select 1",
        backend_id: backend.id,
        source_id: source.id,
        user_id: user.id
      }

      {:error, changeset} = FetchQueries.create_fetch_query(attrs)

      assert errors_on(changeset).cron
    end

    test "requires query for bigquery backends", %{user: user} do
      backend = insert(:backend, type: :bigquery, user: user)
      source = insert(:source, user: user)

      attrs = %{
        name: "test",
        cron: "*/5 * * * *",
        query: nil,
        language: :bq_sql,
        backend_id: backend.id,
        source_id: source.id,
        user_id: user.id
      }

      {:error, changeset} = FetchQueries.create_fetch_query(attrs)

      assert "can't be blank" in errors_on(changeset).query
    end

    test "requires query for webhook backend with jsonpath language", %{user: user} do
      backend =
        insert(:backend,
          type: :webhook,
          user: user,
          config: %{"url" => "https://example.com/api"}
        )

      source = insert(:source, user: user)

      attrs = %{
        name: "test",
        cron: "*/5 * * * *",
        query: nil,
        language: :jsonpath,
        backend_id: backend.id,
        source_id: source.id,
        user_id: user.id
      }

      {:error, changeset} = FetchQueries.create_fetch_query(attrs)

      assert "can't be blank" in errors_on(changeset).query
    end
  end

  describe "get_fetch_query/1" do
    test "retrieves fetch query by id", %{user: user} do
      fetch_query = insert(:fetch_query, user: user)

      result = FetchQueries.get_fetch_query(fetch_query.id)

      assert result.id == fetch_query.id
      assert result.name == fetch_query.name
    end

    test "returns nil for non-existent fetch query" do
      result = FetchQueries.get_fetch_query(999_999)

      assert is_nil(result)
    end
  end

  describe "get_fetch_query_by_external_id/1" do
    test "retrieves fetch query by external_id", %{user: user} do
      fetch_query = insert(:fetch_query, user: user)

      result = FetchQueries.get_fetch_query_by_external_id(fetch_query.external_id)

      assert result.id == fetch_query.id
    end

    test "returns nil for non-existent external_id" do
      result = FetchQueries.get_fetch_query_by_external_id(Ecto.UUID.generate())

      assert is_nil(result)
    end
  end

  describe "list_fetch_queries_by_user_access/1" do
    test "lists fetch queries owned by user", %{user: user} do
      fq1 = insert(:fetch_query, user: user)
      fq2 = insert(:fetch_query, user: user)

      queries = FetchQueries.list_fetch_queries_by_user_access(user)

      assert Enum.any?(queries, &(&1.id == fq1.id))
      assert Enum.any?(queries, &(&1.id == fq2.id))
    end
  end

  describe "update_fetch_query/2" do
    test "updates fetch query", %{user: user} do
      fetch_query = insert(:fetch_query, user: user)

      {:ok, updated} =
        FetchQueries.update_fetch_query(fetch_query, %{
          description: "updated description"
        })

      assert updated.description == "updated description"
    end

    test "validates on update", %{user: user} do
      fetch_query = insert(:fetch_query, user: user)

      {:error, changeset} =
        FetchQueries.update_fetch_query(fetch_query, %{
          cron: "invalid"
        })

      assert errors_on(changeset).cron
    end
  end

  describe "delete_fetch_query/1" do
    test "deletes fetch query", %{user: user} do
      fetch_query = insert(:fetch_query, user: user)

      {:ok, deleted} = FetchQueries.delete_fetch_query(fetch_query)

      assert deleted.id == fetch_query.id
      assert is_nil(FetchQueries.get_fetch_query(fetch_query.id))
    end
  end

  describe "preload_fetch_query/1" do
    test "preloads backend and source", %{user: user} do
      fetch_query = insert(:fetch_query, user: user)

      preloaded = FetchQueries.preload_fetch_query(fetch_query)

      assert preloaded.backend != nil
      assert preloaded.source != nil
      assert preloaded.user != nil
    end
  end

  describe "list_enabled_fetch_queries/0" do
    test "returns only enabled fetch queries", %{user: user} do
      enabled = insert(:fetch_query, enabled: true, user: user)
      insert(:fetch_query, enabled: false, user: user)

      queries = FetchQueries.list_enabled_fetch_queries()

      assert Enum.any?(queries, &(&1.id == enabled.id))
      refute Enum.any?(queries, &(&1.enabled == false))
    end
  end

  describe "trigger_fetch_query_now/1" do
    test "builds job with correct args structure", %{user: user} do
      fetch_query = insert(:fetch_query, user: user)

      # Test that the job is built with correct args using string keys
      job_changeset = %{
        "fetch_query_id" => fetch_query.id,
        "scheduled_at" => DateTime.utc_now() |> DateTime.to_iso8601()
      }
      |> Logflare.FetchQueries.FetchQueryWorker.new(schedule_in: 0)

      assert job_changeset.valid?
      # Check that fetch_query_id is in the args
      args = job_changeset.changes[:args] || job_changeset.data.args
      assert args["fetch_query_id"] == fetch_query.id
    end
  end

  describe "partition_jobs_by_time/1" do
    test "separates future jobs from past jobs" do
      now = DateTime.utc_now()

      future_jobs = [
        %Oban.Job{id: 1, state: "available", scheduled_at: now},
        %Oban.Job{id: 2, state: "scheduled", scheduled_at: now},
        %Oban.Job{id: 3, state: "executing", scheduled_at: now}
      ]

      past_jobs = [
        %Oban.Job{id: 4, state: "completed", scheduled_at: now},
        %Oban.Job{id: 5, state: "discarded", scheduled_at: now},
        %Oban.Job{id: 6, state: "cancelled", scheduled_at: now}
      ]

      all_jobs = future_jobs ++ past_jobs

      {result_future, result_past} = FetchQueries.partition_jobs_by_time(all_jobs)

      assert length(result_future) == 3
      assert length(result_past) == 3

      # Verify correct jobs in each partition
      assert Enum.all?(result_future, &(&1.state in ["available", "scheduled", "executing"]))
      assert Enum.all?(result_past, &(&1.state in ["completed", "discarded", "cancelled"]))
    end

    test "handles empty list" do
      {future, past} = FetchQueries.partition_jobs_by_time([])

      assert future == []
      assert past == []
    end

    test "returns all jobs as future when all are pending" do
      jobs = [
        %Oban.Job{id: 1, state: "available", scheduled_at: DateTime.utc_now()},
        %Oban.Job{id: 2, state: "scheduled", scheduled_at: DateTime.utc_now()}
      ]

      {future, past} = FetchQueries.partition_jobs_by_time(jobs)

      assert length(future) == 2
      assert length(past) == 0
    end

    test "returns all jobs as past when all are completed" do
      jobs = [
        %Oban.Job{id: 1, state: "completed", scheduled_at: DateTime.utc_now()},
        %Oban.Job{id: 2, state: "discarded", scheduled_at: DateTime.utc_now()}
      ]

      {future, past} = FetchQueries.partition_jobs_by_time(jobs)

      assert length(future) == 0
      assert length(past) == 2
    end
  end
end
