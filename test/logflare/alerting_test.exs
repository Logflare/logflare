defmodule Logflare.AlertingTest do
  use Logflare.DataCase, async: false

  alias Logflare.Alerting
  alias Logflare.Alerting.AlertQuery
  alias Logflare.Alerting.AlertsScheduler

  doctest Logflare.SynEventHandler

  setup do
    insert(:plan, name: "Free")

    on_exit(fn ->
      Logflare.Utils.Tasks.kill_all_tasks()
    end)

    {:ok, user: insert(:user)}
  end

  test "cannot start multiple schedulers" do
    AlertsScheduler.start_link(name: :test_scheduler)
    assert {:error, {:already_started, _pid}} = AlertsScheduler.start_link(name: :test_scheduler)
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

    setup do
      start_alerting_supervisor!()
    end

    def alert_query_fixture(user, attrs \\ %{}) do
      attrs = Enum.into(attrs, @valid_attrs)
      {:ok, alert_query} = Alerting.create_alert_query(user, attrs)
      Alerting.sync_alert_jobs()
      alert_query
    end

    test "list_alert_queries/0 returns all alert_queries", %{user: user} do
      alert_query_fixture(user)
      alert_query_fixture(insert(:user))
      assert [_] = Alerting.list_alert_queries(user)
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

      TestUtils.retry_assert(fn ->
        job = Alerting.get_alert_job(alert_query.id)
        assert job
      end)
    end

    test "delete_alert_query/1 deletes the alert_query", %{user: user} do
      alert_query = alert_query_fixture(user)
      assert {:ok, %AlertQuery{}} = Alerting.delete_alert_query(alert_query)

      assert_raise Ecto.NoResultsError, fn -> Alerting.get_alert_query!(alert_query.id) end
      assert nil == Alerting.get_alert_job(alert_query.id)
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

      assert :ok = Alerting.run_alert(alert_query)
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

      assert {:error, :no_results} = Alerting.run_alert(alert_query)
    end

    test "run_alert/2, performs pre-run configuration checks", %{user: user} do
      alert_query = insert(:alert, user: user)

      reject(&GoogleApi.BigQuery.V2.Api.Jobs.bigquery_jobs_query/3)
      reject(&Logflare.Backends.Adaptor.WebhookAdaptor.Client.send/1)
      reject(&Logflare.Backends.Adaptor.SlackAdaptor.Client.send/2)
      Application.get_env(:logflare, Logflare.Alerting)
      cfg = Application.get_env(:logflare, Logflare.Alerting)

      on_exit(fn ->
        Application.put_env(:logflare, Logflare.Alerting, cfg)
      end)

      # min_cluster_size
      Application.put_env(:logflare, Logflare.Alerting, min_cluster_size: 4, enabled: true)
      assert {:error, :below_min_cluster_size} = Alerting.run_alert(alert_query, :scheduled)
      # enabled flag
      Application.put_env(:logflare, Logflare.Alerting, min_cluster_size: 1, enabled: false)
      assert {:error, :not_enabled} = Alerting.run_alert(alert_query, :scheduled)
    end
  end

  describe "quantum integration" do
    setup do
      start_alerting_supervisor!()
    end

    test "upsert_alert_job/1, get_alert_job/1, delete_alert_job/1, count_alert_jobs/0 retrieves alert job",
         %{user: user} do
      %{id: alert_id} = alert = insert(:alert, user_id: user.id)
      {:ok, pid} = Alerting.sync_alert_jobs()

      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, _, _, _}

      assert {:ok,
              %Quantum.Job{
                run_strategy: %Quantum.RunStrategy.Local{},
                task: {Logflare.Alerting, :run_alert, [^alert_id, :scheduled]}
              }} = Alerting.upsert_alert_job(alert)

      assert %Quantum.Job{
               task: {Logflare.Alerting, :run_alert, [^alert_id, :scheduled]}
             } = Alerting.get_alert_job(alert_id)

      assert {:ok, _} = Alerting.delete_alert_job(alert)

      TestUtils.retry_assert(fn ->
        assert {:error, :not_found} = Alerting.delete_alert_job(alert.id)
      end)

      TestUtils.retry_assert(fn -> refute Alerting.get_alert_job(alert_id) end)
    end

    test "upsert alert job multiple times will ensure only one job is present", %{user: user} do
      alert = insert(:alert, user_id: user.id)
      for _ <- 0..10, do: Alerting.upsert_alert_job(alert)
      assert Alerting.list_alert_jobs() |> length == 1
    end

    test "upsert job with new values will replace existing job", %{user: user} do
      alert = insert(:alert, user_id: user.id, cron: "0 0 1 * *")
      Alerting.upsert_alert_job(alert)
      original_job = Alerting.get_alert_job(alert.id)
      assert Alerting.get_alert_job(alert.id)
      new_alert = %{alert | cron: "0 0 2 * *"}
      Alerting.upsert_alert_job(new_alert)
      assert original_job != Alerting.get_alert_job(alert.id)
    end

    test "init/sync functions will populate scheduler with alerts", %{user: user} do
      %{id: alert_id} = insert(:alert, user_id: user.id)

      assert [
               %Quantum.Job{
                 task: {Logflare.Alerting, :run_alert, [^alert_id, :scheduled]}
               }
             ] = Alerting.init_alert_jobs()

      refute Alerting.get_alert_job(alert_id)
      Alerting.sync_alert_jobs()

      TestUtils.retry_assert(fn -> Alerting.get_alert_job(alert_id) end)
    end

    test "sync specific alert by alert_id when not in scheduler", %{user: user} do
      %{id: alert_id} = insert(:alert, user_id: user.id)

      refute Alerting.get_alert_job(alert_id)
      Alerting.sync_alert_job(alert_id)
      TestUtils.retry_assert(fn -> Alerting.get_alert_job(alert_id) end)
    end

    test "sync deleted alert when in scheduler", %{user: user} do
      %{id: alert_id} = insert(:alert, user_id: user.id)
      Alerting.sync_alert_jobs()

      TestUtils.retry_assert(fn -> Alerting.get_alert_job(alert_id) end)
      Repo.delete_all(AlertQuery)
      Alerting.sync_alert_job(alert_id)
      TestUtils.retry_assert(fn -> refute Alerting.get_alert_job(alert_id) end)
    end

    test "supervisor startup will populate scheduler with alerts", %{user: user} do
      %{id: alert_id} = insert(:alert, user_id: user.id)
      Alerting.sync_alert_jobs()

      TestUtils.retry_assert(fn ->
        job_name = Alerting.to_job_name(alert_id)

        assert %Quantum.Job{
                 name: ^job_name,
                 task: {Logflare.Alerting, :run_alert, [^alert_id, :scheduled]}
               } = Alerting.get_alert_job(alert_id)
      end)
    end
  end

  describe "scheduled run_alert/1" do
    setup do
      start_alerting_supervisor!()
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

      :ok = Alerting.sync_alert_job(alert.id)

      {:ok, _updated_alert} =
        Alerting.update_alert_query(alert, %{query: "select 'unique_fresh_data_check'"})

      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, opts ->
        assert opts[:body].query =~ "unique_fresh_data_check"
        {:ok, TestUtils.gen_bq_response([%{"testing" => "123"}])}
      end)

      assert :ok = Alerting.run_alert(alert.id, :scheduled)
    end

    test "run_alert/1 no-ops if alert is missing", %{user: user} do
      {:ok, alert} =
        Alerting.create_alert_query(user, %{
          name: "To Delete",
          cron: "0 0 1 * *",
          query: "select 1"
        })

      :ok = Alerting.sync_alert_job(alert.id)

      # verify job exists
      assert %Quantum.Job{} = Alerting.get_alert_job(alert.id)

      # simulate "external" unsynced deletion / stale state
      Repo.delete!(alert)

      # expect NO BQ execution
      GoogleApi.BigQuery.V2.Api.Jobs
      |> reject(:bigquery_jobs_query, 3)

      # verify job is NOT run by stale run_alert
      assert {:error, :not_found} = Alerting.run_alert(alert.id, :scheduled)
    end
  end

  describe "sync_alert_jobs/0" do
    setup do
      start_alerting_supervisor!()
    end

    defp await_sync_alert_jobs do
      {:ok, pid} = Alerting.sync_alert_jobs()
      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, :process, _pid, _reason}
    end

    test "adds missing jobs", %{user: user} do
      alert = insert(:alert, user: user)
      Alerting.delete_alert_job(alert.id)

      refute Alerting.get_alert_job(alert.id)
      await_sync_alert_jobs()
      assert Alerting.get_alert_job(alert.id)
    end

    test "removes obsolete jobs", %{user: user} do
      alert = insert(:alert, user: user)
      Alerting.sync_alert_job(alert.id)
      assert Alerting.get_alert_job(alert.id)

      # simulate "external" unsynced deletion / stale state
      Repo.delete!(alert)

      assert Alerting.get_alert_job(alert.id)
      await_sync_alert_jobs()
      refute Alerting.get_alert_job(alert.id)
    end

    test "updates jobs with changed schedule", %{user: user} do
      alert = insert(:alert, user: user, cron: "0 0 1 * *")
      Alerting.sync_alert_job(alert.id)
      original_job = Alerting.get_alert_job(alert.id)

      {:ok, _} = Alerting.update_alert_query(alert, %{cron: "*/5 * * * *"})

      assert Alerting.get_alert_job(alert.id) == original_job
      await_sync_alert_jobs()
      assert Alerting.get_alert_job(alert.id) != original_job
    end
  end

  defp start_alerting_supervisor! do
    start_supervised!(Logflare.Alerting.Supervisor)
    # wait for scheduler init to finish
    :timer.sleep(500)
  end
end
