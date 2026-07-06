defmodule LogflareWeb.Api.AlertControllerTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  setup %{conn: conn} do
    insert(:plan, name: "Free")
    user = insert(:user)
    conn = add_access_token(conn, user, "private")
    backend = insert(:backend, user: user)
    {:ok, conn: conn, user: user, backend: backend}
  end

  test "list alerts", %{conn: conn, user: user} do
    insert(:alert, user: insert(:user))
    alert = insert(:alert, user: user)

    assert [result] =
             conn
             |> get(~p"/api/alerts")
             |> json_response(200)

    assert result["id"] == alert.id
    assert result["token"] == alert.token
  end

  test "show alert", %{conn: conn, user: user} do
    alert = insert(:alert, user: user)
    insert(:alert, user: user)

    assert result =
             conn
             |> get(~p"/api/alerts/#{alert.token}")
             |> json_response(200)

    assert result["id"] == alert.id
    assert result["token"] == alert.token
    assert result["name"] == alert.name
  end

  describe "with alerts" do
    test "create alert", %{conn: conn} do
      assert %{"token" => alert_token} =
               conn
               |> post(~p"/api/alerts", %{
                 name: "my alert",
                 query: "select current_date() as date",
                 cron: "0 0 1 * *",
                 language: "bq_sql"
               })
               |> json_response(201)

      assert alert_token

      assert conn
             |> get(~p"/api/alerts/#{alert_token}")
             |> json_response(200)
    end

    test "create alert with bad params", %{conn: conn} do
      assert %{"errors" => _} =
               conn
               |> post(~p"/api/alerts", %{name: "missing required fields"})
               |> json_response(422)
    end

    test "update alert", %{conn: conn, user: user} do
      alert = insert(:alert, user: user, name: "initial")

      assert %{"token" => alert_token} =
               conn
               |> put(~p"/api/alerts/#{alert.token}", %{name: "adjusted"})
               |> json_response(200)

      assert %{"name" => "adjusted"} =
               conn
               |> get(~p"/api/alerts/#{alert_token}")
               |> json_response(200)
    end

    test "update alert to attach an owned backend", %{
      conn: conn,
      user: user,
      backend: backend
    } do
      alert = insert(:alert, user: user)

      assert %{"token" => alert_token} =
               conn
               |> put(~p"/api/alerts/#{alert.token}", %{backend_ids: [backend.id]})
               |> json_response(200)

      updated =
        Logflare.Alerting.get_alert_query!(alert.id)
        |> Logflare.Alerting.preload_alert_query()

      assert alert_token == updated.token
      assert [%{id: backend_id}] = updated.backends
      assert backend_id == backend.id
    end

    test "attacker cannot attach a victim's backend to their alert", %{
      conn: conn,
      user: user
    } do
      alert = insert(:alert, user: user)

      victim = insert(:user)
      victim_backend = insert(:backend, user: victim)

      conn
      |> put(~p"/api/alerts/#{alert.token}", %{backend_ids: [victim_backend.id]})
      |> json_response(404)

      reloaded =
        Logflare.Alerting.get_alert_query!(alert.id)
        |> Logflare.Alerting.preload_alert_query()

      assert reloaded.backends == []
    end

    test "update alert with bad user", %{conn: conn, user: user} do
      other_user = insert(:user)
      alert = insert(:alert, user: user, name: "initial")

      assert %{"error" => _} =
               conn
               |> add_access_token(other_user, "private")
               |> put(~p"/api/alerts/#{alert.token}", %{name: "adjusted"})
               |> json_response(404)

      assert %{"name" => "initial"} =
               conn
               |> get(~p"/api/alerts/#{alert.token}")
               |> json_response(200)
    end

    test "delete alert", %{conn: conn, user: user} do
      alert = insert(:alert, user: user)

      assert conn
             |> delete(~p"/api/alerts/#{alert.token}")
             |> response(204)

      assert conn
             |> get(~p"/api/alerts/#{alert.token}")
             |> json_response(404)
    end

    test "delete alert with bad user", %{conn: conn, user: user} do
      other_user = insert(:user)
      alert = insert(:alert, user: user)

      assert conn
             |> add_access_token(other_user, "private")
             |> delete(~p"/api/alerts/#{alert.token}")
             |> response(404)

      assert conn
             |> get(~p"/api/alerts/#{alert.token}")
             |> json_response(200)
    end
  end
end
