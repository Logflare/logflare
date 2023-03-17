defmodule LogflareWeb.AlertsLiveTest do
  use LogflareWeb.ConnCase
  import Phoenix.LiveViewTest
  alias Logflare.Alerting

  @create_attrs %{
    name: "some name",
    cron: "1 * * * * *",
    active: true,
    query: "select id from `my-source`",
    slack_hook_url: "some slack_hook_url",
    source_mapping: %{},
    webhook_notification_url: "some webhook_notification_url"
  }
  @update_attrs %{
    name: "some updated name",
    cron: "2 * * * * *",
    active: false,
    query: "select another from `my-source`",
    slack_hook_url: "some updated slack_hook_url",
    source_mapping: %{},
    webhook_notification_url: "some updated webhook_notification_url"
  }
  @invalid_attrs %{
    name: "invalid name",
    cron: nil,
    active: nil,
    query: nil,
    slack_hook_url: nil,
    source_mapping: nil,
    token: nil,
    webhook_notification_url: nil
  }

  setup %{conn: conn} do
    insert(:plan, name: "Free")
    user = insert(:user)

    conn =
      conn
      |> login_user(user)

    [user: user, conn: conn]
  end

  defp create_alert_query(%{user: user}) do
    {:ok, alert_query} = Alerting.create_alert_query(user, @create_attrs)
    %{alert_query: alert_query}
  end

  describe "Index" do
    setup [:create_alert_query]

    test "lists all alert_queries", %{conn: conn, alert_query: alert_query} do
      {:ok, _view, html} = live(conn, Routes.alerts_path(conn, :index))
      assert html =~ alert_query.name
    end

    test "saves new alert_query", %{conn: conn} do
      {:ok, view, _html} = live(conn, Routes.alerts_path(conn, :index))

      view
      |> render_hook(:create, %{alert_query: @create_attrs})

      assert render(view) =~ @create_attrs.name

      view
      |> render_hook(:create, %{alert_query: @invalid_attrs})

      refute render(view) =~ @invalid_attrs.name
    end

    test "update alert_query", %{conn: conn, alert_query: alert_query} do
      {:ok, view, _html} = live(conn, Routes.alerts_path(conn, :index))

      view
      |> render_hook(:update, %{id: alert_query.id, alert_query: @update_attrs})

      html = render(view)
      assert html =~ @update_attrs.name
      assert html =~ @update_attrs.query
      refute html =~ alert_query.name
      refute html =~ alert_query.query
    end

    test "deletes alert_query in listing", %{conn: conn, alert_query: alert_query} do
      {:ok, view, _html} = live(conn, Routes.alerts_path(conn, :index))

      assert render(view) =~ alert_query.name

      view
      |> render_hook(:delete, %{id: alert_query.id})

      refute render(view) =~ alert_query.name
    end
  end
end
