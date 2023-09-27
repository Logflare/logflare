defmodule Logflare.AlertingTest do
  use Logflare.DataCase

  alias Logflare.Alerting
  alias Logflare.AlertsScheduler

  setup do
    stub(Goth, :fetch, fn _mod -> {:ok, %Goth.Token{token: "auth-token"}} end)
    insert(:plan, name: "Free")
    start_supervised!(AlertsScheduler)
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

    test "create_alert_query/1 with invalid data returns error changeset", %{user: user} do
      assert {:error, %Ecto.Changeset{}} = Alerting.create_alert_query(user, @invalid_attrs)
      # invalid cron
      assert {:error, %Ecto.Changeset{}} =
               Alerting.create_alert_query(user, %{@valid_attrs | cron: "something"})

      # less than 15 mins
      assert {:error, %Ecto.Changeset{}} =
               Alerting.create_alert_query(user, %{@valid_attrs | cron: "* * * * *"})

      assert {:error, %Ecto.Changeset{}} =
               Alerting.create_alert_query(user, %{@valid_attrs | cron: "*/10 * * * *"})

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

    test "change_alert_query/1 returns a alert_query changeset", %{user: user} do
      alert_query = alert_query_fixture(user)
      assert %Ecto.Changeset{} = Alerting.change_alert_query(alert_query)
    end

    test "execute_alert_query", %{user: user} do
      alert_query = insert(:alert, user: user) |> Logflare.Repo.preload([:user])

      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"testing" => "123"}])}
      end)

      assert {:ok, [%{"testing" => "123"}]} = Alerting.execute_alert_query(alert_query)
    end

    test "run_alert_query/1 runs the entire alert", %{user: user} do
      alert_query = insert(:alert, user: user)

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"testing" => "123"}])}
      end)

      Logflare.Backends.Adaptor.WebhookAdaptor.Client
      |> expect(:send, fn _url, %{"result" => _} = _body -> :ok end)

      Logflare.Backends.Adaptor.SlackAdaptor.Client
      |> expect(:send, fn _url, %{blocks: _} = _records -> :ok end)

      assert :ok = Alerting.run_alert(alert_query)
    end
  end

  describe "citrine integration" do
    test "upsert_alert_job/1, get_alert_job/1, delete_alert_job/1, count_alert_jobs/0 retrieves alert job",
         %{user: user} do
      alert = insert(:alert, user_id: user.id)
      alert_id = alert.id
      assert {:ok, %Citrine.Job{id: ^alert_id}} = Alerting.upsert_alert_job(alert)
      # create a citrine job
      assert %Citrine.Job{id: ^alert_id} = Alerting.get_alert_job(alert_id)

      assert :ok = Alerting.delete_alert_job(alert)
      assert :ok = Alerting.delete_alert_job(alert.id)

      assert nil == Alerting.get_alert_job(alert_id)
    end

    test "init function will populate citrine with alerts", %{user: user} do
      alert = insert(:alert, user_id: user.id)
      assert nil == Alerting.get_alert_job(alert.id)
      assert :ok = Alerting.init_alert_jobs()
      assert %Citrine.Job{} = Alerting.get_alert_job(alert.id)
    end
  end
end
