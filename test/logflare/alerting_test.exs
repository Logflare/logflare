defmodule Logflare.AlertingTest do
  use Logflare.DataCase

  alias Logflare.Alerting

  setup do
    {:ok, user: insert(:user)}
  end

  describe "alert_queries" do
    alias Logflare.Alerting.AlertQuery

    @valid_attrs %{
      name: "some name",
      cron: "1 * * * * *",
      query: "select id from `some-source`",
      slack_hook_url: "some slack_hook_url",
      source_mapping: %{},
      token: "7488a646-e31f-11e4-aace-600308960662",
      webhook_notification_url: "some webhook_notification_url"
    }
    @update_attrs %{
      name: "some updated name",
      cron: "2 * * * * *",
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
  end
end
