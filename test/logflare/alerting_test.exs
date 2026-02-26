defmodule Logflare.AlertingTest do
  use Logflare.DataCase, async: false
  use Oban.Testing, repo: Logflare.Repo

  alias Logflare.Alerting
  alias Logflare.Alerting.AlertQuery
  alias Logflare.Alerting.AlertWorker

  doctest Logflare.SynEventHandler

  setup do
    insert(:plan, name: "Free")

    on_exit(fn ->
      Logflare.Utils.Tasks.kill_all_tasks()
    end)

    {:ok, user: insert(:user)}
  end

  describe "alert_queries" do
    alias Logflare.Alerting.AlertQuery

    @valid_attrs %{
      name: "some name",
      cron: "0 0 1 * *",
      query: "select id from `some-source`",
      slack_hook_url: "some slack_hook_url",
      source_mapping: %{},
      token: "7488a646-e31f-11e4-aace-600308960662",
      webhook_notification_url: "some webhook_notification_url"
    }
    @update_attrs %{
      name: "some updated name",
      cron: "0 0 1 1 *",
      query: "select other from `some-source`",
      slack_hook_url: "some updated slack_hook_url",
      source_mapping: %{},
      token: "7488a646-e31f-11e4-aace-600308960668",
      webhook_notification_url: "some updated webhook_notification_url"
    }
    @invalid_attrs %{
      name: nil,
      query: nil,
      cron: nil,
      slack_hook_url: nil,
      source_mapping: nil,
      token: nil,
      webhook_notification_url: nil
    }

    def alert_query_fixture(user, attrs \\ %{}) do
      attrs = Enum.into(attrs, @valid_attrs)
      {:ok, alert_query} = Alerting.create_alert_query(user, attrs)
      alert_query
    end

    test "list_alert_queries/0 returns all alert_queries", %{user: user} do
      alert_query_fixture(user)
      alert_query_fixture(insert(:user))
      assert [_] = Alerting.list_alert_queries(user)
    end

    test "list_all_alert_queries/0 returns all alert queries across users", %{user: user} do
      alert_query_fixture(user)
      alert_query_fixture(insert(:user))
      assert length(Alerting.list_all_alert_queries()) >= 2
    end

    test "list_all_alert_queries/0 excludes disabled alerts", %{user: user} do
      enabled_alert = alert_query_fixture(user)
      disabled_alert = alert_query_fixture(user, %{name: "disabled alert"})
      Alerting.update_alert_query(disabled_alert, %{enabled: false})

      results = Alerting.list_all_alert_queries()
      result_ids = Enum.map(results, & &1.id)

      assert enabled_alert.id in result_ids
      refute disabled_alert.id in result_ids
    end

    test "list_alert_queries_user_access" do
      user = insert(:user)
      team_user = insert(:team_user, email: user.email)

      %AlertQuery{id: alert_id} = insert(:alert, user: user)
      %AlertQuery{id: other_alert_id} = insert(:alert, user: team_user.team.user)
      %AlertQuery{id: forbidden_alert_id} = insert(:alert, user: build(:user))

      alert_ids =
        Alerting.list_alert_queries_user_access(user)
        |> Enum.map(& &1.id)

      assert alert_id in alert_ids
      assert other_alert_id in alert_ids
      refute forbidden_alert_id in alert_ids
    end

    test "get_alert_query_by_user_access/2" do
      owner = insert(:user)
      team_user = insert(:team_user, email: owner.email)
      %AlertQuery{id: alert_id} = insert(:alert, user: owner)
      %AlertQuery{id: other_alert_id} = insert(:alert, user: team_user.team.user)
      %AlertQuery{id: forbidden_alert_id} = insert(:alert, user: build(:user))

      assert %AlertQuery{id: ^alert_id} =
               Alerting.get_alert_query_by_user_access(owner, alert_id)

      assert %AlertQuery{id: ^alert_id} =
               Alerting.get_alert_query_by_user_access(team_user, alert_id)

      assert %AlertQuery{id: ^other_alert_id} =
               Alerting.get_alert_query_by_user_access(team_user, other_alert_id)

      assert nil == Alerting.get_alert_query_by_user_access(owner, forbidden_alert_id)
      assert nil == Alerting.get_alert_query_by_user_access(team_user, forbidden_alert_id)
    end

    test "get_alert_query!/1 returns the alert_query with given id", %{user: user} do
      alert_query = alert_query_fixture(user)
      alert_query_fixture(insert(:user))
      assert Alerting.get_alert_query!(alert_query.id).id == alert_query.id
    end

    test "create_alert_query/1 with valid data creates a alert_query", %{user: user} do
      assert {:ok, %AlertQuery{} = alert_query} = Alerting.create_alert_query(user, @valid_attrs)
      assert alert_query.user_id
      assert alert_query.name == @valid_attrs.name
      assert alert_query.query == @valid_attrs.query
      assert alert_query.slack_hook_url == @valid_attrs.slack_hook_url
      assert alert_query.webhook_notification_url == @valid_attrs.webhook_notification_url
      assert alert_query.source_mapping == %{}
      assert alert_query.token
    end

    test "bug: create_alert_query/1 with very long query", %{user: user} do
      assert {:ok, %AlertQuery{}} =
               Alerting.create_alert_query(user, %{
                 @valid_attrs
                 | query: """
                   with pg as (
                    select round(count(t.id) / 360 ) as rate from `postgres.logs` t
                    where   t.timestamp > timestamp_sub(current_timestamp(), interval 5 minute)
                   ), cf as (
                    select round(count(t.id) / 360 ) as rate from `cloudflare.logs.prod` t
                    where   t.timestamp > timestamp_sub(current_timestamp(), interval 5 minute)
                   )
                   select pg.rate as pg_per_sec, cf.rate as cf_per_sec from pg, cf
                   """
               })
    end

    test "create_alert_query/1 with invalid data returns error changeset", %{user: user} do
      assert {:error, %Ecto.Changeset{}} = Alerting.create_alert_query(user, @invalid_attrs)
      # invalid cron
      assert {:error, %Ecto.Changeset{}} =
               Alerting.create_alert_query(user, %{@valid_attrs | cron: "something"})

      # less than 5 mins
      assert {:error, %Ecto.Changeset{}} =
               Alerting.create_alert_query(user, %{@valid_attrs | cron: "* * * * *"})

      assert {:error, %Ecto.Changeset{}} =
               Alerting.create_alert_query(user, %{@valid_attrs | cron: "*/3 * * * *"})

      # second precision extended syntax
      assert {:error, %Ecto.Changeset{}} =
               Alerting.create_alert_query(user, %{@valid_attrs | cron: "* * * * * *"})
    end

    test "update_alert_query/2 with valid data updates the alert_query", %{user: user} do
      alert_query = alert_query_fixture(user)

      assert {:ok, %AlertQuery{} = alert_query} =
               Alerting.update_alert_query(alert_query, @update_attrs)

      assert alert_query.name == @update_attrs.name
      assert alert_query.query == @update_attrs.query
      assert alert_query.slack_hook_url == @update_attrs.slack_hook_url
      assert alert_query.webhook_notification_url == @update_attrs.webhook_notification_url
    end

    test "update_alert_query/2 with invalid data returns error changeset", %{user: user} do
      alert_query = alert_query_fixture(user)

      assert {:error, %Ecto.Changeset{}} =
               Alerting.update_alert_query(alert_query, @invalid_attrs)

      assert alert_query.updated_at == Alerting.get_alert_query!(alert_query.id).updated_at
    end

    test "delete_alert_query/1 deletes the alert_query", %{user: user} do
      alert_query = alert_query_fixture(user)
      assert {:ok, %AlertQuery{}} = Alerting.delete_alert_query(alert_query)

      assert_raise Ecto.NoResultsError, fn -> Alerting.get_alert_query!(alert_query.id) end
    end

    test "delete_alert_query/1 deletes associated Oban jobs", %{user: user} do
      alert_query = alert_query_fixture(user)
      other_alert = alert_query_fixture(user)

      # Insert jobs for the alert being deleted
      for state <- ~w(scheduled available completed) do
        insert_oban_job(alert_query, %{state: state})
      end

      # Insert jobs for another alert (should not be affected)
      for state <- ~w(scheduled available completed) do
        insert_oban_job(other_alert, %{state: state})
      end

      assert {:ok, %AlertQuery{}} = Alerting.delete_alert_query(alert_query)

      # All jobs for the deleted alert should be gone
      assert Alerting.list_execution_history(alert_query.id) == []

      # Jobs for the other alert should remain
      assert length(Alerting.list_execution_history(other_alert.id)) == 3
    end

    test "change_alert_query/1 returns a alert_query changeset", %{user: user} do
      alert_query = alert_query_fixture(user)
      assert %Ecto.Changeset{} = Alerting.change_alert_query(alert_query)
    end

    test "execute_alert_query", %{user: user} do
      alert_query = insert(:alert, user: user) |> Logflare.Repo.preload([:user])

      pid = self()

      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, opts ->
        send(pid, {:reservation, opts[:body].reservation})
        {:ok, TestUtils.gen_bq_response([%{"testing" => "123"}])}
      end)

      assert {:ok, %{rows: [%{"testing" => "123"}], total_bytes_processed: _}} =
               Alerting.execute_alert_query(alert_query)

      #  no reservation set by user
      assert_receive {:reservation, nil}
    end

    test "execute_alert_query with reservation" do
      user =
        insert(:user, bigquery_reservation_alerts: "projects/1234567890/reservations/1234567890")

      alert_query = insert(:alert, user: user) |> Logflare.Repo.preload([:user])
      pid = self()

      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, opts ->
        send(pid, {:reservation, opts[:body].reservation})
        {:ok, TestUtils.gen_bq_response([%{"testing" => "123"}])}
      end)

      assert {:ok, %{rows: [%{"testing" => "123"}], total_bytes_processed: _}} =
               Alerting.execute_alert_query(alert_query)

      assert_receive {:reservation, reservation}
      assert reservation == user.bigquery_reservation_alerts
    end

    test "execute_alert_query with query composition" do
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

      alert_query =
        insert(:alert, user: user, query: "select testing from `my.date`")
        |> Logflare.Repo.preload([:user])

      assert {:ok, %{rows: [%{"testing" => "123"}]}} =
               Alerting.execute_alert_query(alert_query)
    end

    test "run_alert_query/1 runs the entire alert", %{user: user} do
      alert_query = insert(:alert, user: user)

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"testing" => "123"}])}
      end)

      Logflare.Backends.Adaptor.WebhookAdaptor.Client
      |> expect(:send, fn opts ->
        assert Map.has_key?(opts[:body], "result")
        {:ok, %Tesla.Env{}}
      end)

      Logflare.Backends.Adaptor.SlackAdaptor.Client
      |> expect(:send, fn _url, body ->
        assert Map.has_key?(body, :blocks)
        {:ok, %Tesla.Env{}}
      end)

      assert {:ok,
              %{
                total_rows: 1,
                total_bytes_processed: _,
                fired: true,
                rows: [%{"testing" => "123"}]
              }} = Alerting.run_alert(alert_query)
    end

    test "run_alert_query/1 does not send notifications if no results", %{user: user} do
      alert_query = insert(:alert, user: user)

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([])}
      end)

      Logflare.Backends.Adaptor.WebhookAdaptor.Client
      |> reject(:send, 1)

      Logflare.Backends.Adaptor.SlackAdaptor.Client
      |> reject(:send, 2)

      assert {:ok,
              %{
                total_rows: 0,
                total_bytes_processed: _,
                fired: false,
                rows: []
              }} = Alerting.run_alert(alert_query)
    end
  end

  describe "scheduled run_alert/1" do
    setup do
      old_config = Application.get_env(:logflare, Logflare.Alerting)
      Application.put_env(:logflare, Logflare.Alerting, min_cluster_size: 0, enabled: true)
      on_exit(fn -> Application.put_env(:logflare, Logflare.Alerting, old_config) end)
    end

    test "uses fresh alert query data", %{user: user} do
      {:ok, alert} =
        Alerting.create_alert_query(user, %{
          name: "Original Name",
          cron: "0 0 1 * *",
          query: "select 1"
        })

      {:ok, _updated_alert} =
        Alerting.update_alert_query(alert, %{query: "select 'unique_fresh_data_check'"})

      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, opts ->
        assert opts[:body].query =~ "unique_fresh_data_check"
        {:ok, TestUtils.gen_bq_response([%{"testing" => "123"}])}
      end)

      assert {:ok,
              %{
                total_rows: 1,
                total_bytes_processed: _,
                fired: true,
                rows: [%{"testing" => "123"}]
              }} = Alerting.run_alert(alert.id, :scheduled)
    end

    test "run_alert/1 no-ops if alert is missing", %{user: user} do
      {:ok, alert} =
        Alerting.create_alert_query(user, %{
          name: "To Delete",
          cron: "0 0 1 * *",
          query: "select 1"
        })

      # simulate "external" unsynced deletion / stale state
      Repo.delete!(alert)

      # expect NO BQ execution
      GoogleApi.BigQuery.V2.Api.Jobs
      |> reject(:bigquery_jobs_query, 3)

      # verify job is NOT run by stale run_alert
      assert {:error, :not_found} = Alerting.run_alert(alert.id, :scheduled)
    end
  end

  describe "trigger_alert_now/1" do
    test "enqueues an immediate AlertWorker job", %{user: user} do
      alert = insert(:alert, user: user)
      assert {:ok, %Oban.Job{}} = Alerting.trigger_alert_now(alert)
      assert_enqueued(worker: AlertWorker, args: %{alert_query_id: alert.id})
    end
  end

  describe "list_execution_history/1" do
    test "returns empty list when no jobs exist", %{user: user} do
      alert = insert(:alert, user: user)
      assert [] = Alerting.list_execution_history(alert.id)
    end
  end

  defp insert_oban_job(alert_query, attrs) do
    now = DateTime.utc_now()

    defaults = %{
      state: "completed",
      queue: "alerts",
      worker: "Logflare.Alerting.AlertWorker",
      args: %{"alert_query_id" => alert_query.id, "scheduled_at" => DateTime.to_iso8601(now)},
      meta: %{},
      attempted_at: now,
      completed_at: now,
      scheduled_at: now,
      attempted_by: ["test_node"],
      max_attempts: 1
    }

    merged = Map.merge(defaults, attrs)

    %Oban.Job{}
    |> Ecto.Changeset.change(merged)
    |> Repo.insert!()
  end

  describe "enabled field" do
    test "disabling deletes future jobs, keeps past jobs, ignores other alerts", %{user: user} do
      alert = insert(:alert, user: user)
      other_alert = insert(:alert, user: user, name: "other alert")

      for state <- ~w(scheduled available executing), do: insert_oban_job(alert, %{state: state})
      for state <- ~w(completed discarded), do: insert_oban_job(alert, %{state: state})
      insert_oban_job(other_alert, %{state: "scheduled"})

      assert length(Alerting.list_future_jobs(alert.id)) == 3

      assert {:ok, %{enabled: false}} =
               Alerting.update_alert_query(alert, %{enabled: false})

      assert Alerting.list_future_jobs(alert.id) == []
      past_states = Alerting.list_execution_history(alert.id) |> Enum.map(& &1.state)
      assert "completed" in past_states
      assert "discarded" in past_states
      assert length(Alerting.list_future_jobs(other_alert.id)) == 1
    end

    test "enabling schedules 5 future jobs with correct args", %{user: user} do
      alert = insert(:alert, user: user, cron: "0 0 * * *", enabled: false)
      assert Alerting.list_future_jobs(alert.id) == []

      {:ok, %{enabled: true}} = Alerting.update_alert_query(alert, %{enabled: true})
      future_jobs = Alerting.list_future_jobs(alert.id)
      assert length(future_jobs) == 5
      now = DateTime.utc_now()

      for job <- future_jobs do
        assert job.args["alert_query_id"] == alert.id
        assert DateTime.compare(job.scheduled_at, now) == :gt
      end
    end

    test "unrelated update does not affect jobs", %{user: user} do
      alert = insert(:alert, user: user)
      insert_oban_job(alert, %{state: "scheduled"})

      assert {:ok, %{name: "renamed"}} = Alerting.update_alert_query(alert, %{name: "renamed"})
      assert length(Alerting.list_future_jobs(alert.id)) == 1
    end

    test "schedule_alert/1 handles invalid cron gracefully", %{user: user} do
      alert = insert(:alert, user: user)
      assert :ok = Alerting.schedule_alert(%{alert | cron: "invalid"})
      assert Alerting.list_future_jobs(alert.id) == []
    end
  end

  describe "partition_jobs_by_time/1" do
    test "partitions by state" do
      now = DateTime.utc_now()

      scheduled_job = %Oban.Job{state: "scheduled", scheduled_at: now}
      completed_job = %Oban.Job{state: "completed", scheduled_at: now}
      executing_job = %Oban.Job{state: "executing", scheduled_at: now}
      discarded_job = %Oban.Job{state: "discarded", scheduled_at: now}

      result =
        Alerting.partition_jobs_by_time([
          scheduled_job,
          completed_job,
          executing_job,
          discarded_job
        ])

      assert length(result.future) == 2
      assert length(result.past) == 2
      assert scheduled_job in result.future
      assert executing_job in result.future
      assert completed_job in result.past
      assert discarded_job in result.past
    end
  end
end
