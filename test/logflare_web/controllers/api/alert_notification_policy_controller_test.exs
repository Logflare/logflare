defmodule LogflareWeb.Api.AlertNotificationPolicyControllerTest do
  use LogflareWeb.ConnCase

  alias Logflare.AlertIssues.NotificationPolicies

  setup %{conn: conn} do
    insert(:plan, name: "Free")
    user = insert(:user)
    {:ok, conn: add_access_token(conn, user, "private"), user: user}
  end

  @attrs %{
    "name" => "advisor-pages",
    "rule_keys" => ["supabase.advisor.*"],
    "min_severity" => "major",
    "categories" => ["security"],
    "identifiers" => %{"project" => "default"},
    "events" => ["opened", "repeat"],
    "repeat_interval_seconds" => 3600,
    "receivers" => ["ops-webhook"]
  }

  test "creates, reads, updates and deletes a notification policy", %{conn: conn} do
    created =
      conn
      |> post(~p"/api/alerts/notification_policies", @attrs)
      |> json_response(201)

    assert %{
             "id" => id,
             "name" => "advisor-pages",
             "enabled" => true,
             "rule_keys" => ["supabase.advisor.*"],
             "min_severity" => "major",
             "categories" => ["security"],
             "identifiers" => %{"project" => "default"},
             "events" => ["opened", "repeat"],
             "repeat_interval_seconds" => 3600,
             "receivers" => ["ops-webhook"]
           } = created

    assert [%{"id" => ^id}] =
             conn |> get(~p"/api/alerts/notification_policies") |> json_response(200)

    assert %{"id" => ^id} =
             conn |> get(~p"/api/alerts/notification_policies/#{id}") |> json_response(200)

    assert %{"enabled" => false, "min_severity" => nil} =
             conn
             |> put(~p"/api/alerts/notification_policies/#{id}", %{
               "enabled" => false,
               "min_severity" => nil
             })
             |> json_response(200)

    assert conn |> delete(~p"/api/alerts/notification_policies/#{id}") |> response(204)
    assert conn |> get(~p"/api/alerts/notification_policies/#{id}") |> json_response(404)
  end

  test "rejects an invalid policy", %{conn: conn} do
    assert %{"errors" => %{"receivers" => _, "events" => _}} =
             conn
             |> post(~p"/api/alerts/notification_policies", %{
               "name" => "bad",
               "events" => ["paged"],
               "receivers" => []
             })
             |> json_response(422)
  end

  test "another user's policies are not found", %{conn: conn} do
    other = insert(:user)
    {:ok, policy} = NotificationPolicies.create(other, @attrs)

    assert conn |> get(~p"/api/alerts/notification_policies/#{policy.id}") |> json_response(404)

    assert conn
           |> put(~p"/api/alerts/notification_policies/#{policy.id}", %{"enabled" => false})
           |> json_response(404)

    assert conn
           |> delete(~p"/api/alerts/notification_policies/#{policy.id}")
           |> json_response(404)
  end
end
