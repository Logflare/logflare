defmodule LogflareWeb.Api.AlertControllerTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  alias Logflare.Alerting
  alias LogflareWeb.OpenApiSchemas.AlertApiCreateParams
  alias LogflareWeb.OpenApiSchemas.AlertApiSchema
  alias LogflareWeb.OpenApiSchemas.AlertApiUpdateParams
  alias OpenApiSpex.Schema

  setup %{conn: conn} do
    insert(:plan, name: "Free")
    user = insert(:user)
    conn = add_access_token(conn, user, "private")
    backend = insert(:backend, user: user)
    {:ok, conn: conn, user: user, backend: backend}
  end

  test "list alerts", %{conn: conn, user: user, backend: backend} do
    insert(:alert, user: insert(:user))
    alert = insert(:alert, user: user)
    {:ok, alert} = Logflare.Alerting.update_alert_query(alert, %{backends: [backend]})

    response =
      conn
      |> get(~p"/api/alerts")
      |> json_response(200)

    assert [result] = response
    assert result["id"] == alert.id
    assert result["token"] == alert.token
    assert %{"id" => backend_id} = hd(result["backends"])
    assert backend_id == backend.id
  end

  test "show alert", %{conn: conn, user: user} do
    alert = insert(:alert, user: user)
    insert(:alert, user: user)

    result =
      conn
      |> get(~p"/api/alerts/#{alert.token}")
      |> json_response(200)

    assert result["id"] == alert.id
    assert result["token"] == alert.token
    assert result["name"] == alert.name
  end

  test "documents alert request and response schemas" do
    assert AlertApiCreateParams.schema().required == [:name, :query, :cron]
    assert AlertApiUpdateParams.schema().required == []

    for schema <- [AlertApiCreateParams, AlertApiUpdateParams] do
      assert %{
               backend_ids: %Schema{
                 type: :array,
                 items: %Schema{type: :integer, minimum: 1}
               }
             } = schema.schema().properties

      refute Map.has_key?(schema.schema().properties, :backends)
    end

    assert %{backends: %Schema{type: :array}} = AlertApiSchema.schema().properties
  end

  describe "with alerts" do
    test "create alert", %{conn: conn} do
      response =
        conn
        |> post(~p"/api/alerts", %{
          name: "my alert",
          query: "select current_date() as date",
          cron: "0 0 1 * *",
          language: "bq_sql"
        })
        |> json_response(201)

      assert %{"token" => alert_token} = response

      conn
      |> get(~p"/api/alerts/#{alert_token}")
      |> json_response(200)
    end

    test "create alert defaults language when omitted", %{conn: conn} do
      assert %{"language" => "bq_sql"} =
               conn
               |> post(~p"/api/alerts", %{
                 name: "my alert",
                 query: "select current_date() as date",
                 cron: "0 0 1 * *"
               })
               |> json_response(201)
    end

    test "create alert with an owned backend", %{conn: conn, backend: backend} do
      response =
        conn
        |> post(~p"/api/alerts", %{
          name: "my alert",
          query: "select current_date() as date",
          cron: "0 0 1 * *",
          language: "bq_sql",
          backend_ids: [backend.id]
        })
        |> json_response(201)

      assert %{"token" => alert_token, "backends" => [%{"id" => backend_id}]} = response
      assert backend_id == backend.id

      assert %{"backends" => [%{"id" => ^backend_id}]} =
               conn
               |> get(~p"/api/alerts/#{alert_token}")
               |> json_response(200)
    end

    test "rejects backend objects when creating", %{conn: conn, backend: backend} do
      assert %{"error" => "backends is read-only; use backend_ids"} =
               conn
               |> post(~p"/api/alerts", %{
                 name: "my alert",
                 query: "select current_date() as date",
                 cron: "0 0 1 * *",
                 language: "bq_sql",
                 backends: [%{id: backend.id}]
               })
               |> json_response(400)
    end

    test "attacker cannot create an alert with a victim's backend", %{conn: conn, user: user} do
      victim_backend = insert(:backend, user: insert(:user))

      assert %{"error" => "Not Found"} =
               conn
               |> post(~p"/api/alerts", %{
                 name: "my alert",
                 query: "select current_date() as date",
                 cron: "0 0 1 * *",
                 backend_ids: [victim_backend.id]
               })
               |> json_response(404)

      assert Alerting.list_alert_queries(user) == []
    end

    test "create alert with bad params", %{conn: conn} do
      response =
        conn
        |> post(~p"/api/alerts", %{name: "missing required fields"})
        |> json_response(422)

      assert %{"errors" => %{"query" => _, "cron" => _}} = response
    end

    test "update alert", %{conn: conn, user: user} do
      alert = insert(:alert, user: user, name: "initial")

      response =
        conn
        |> put(~p"/api/alerts/#{alert.token}", %{name: "adjusted"})
        |> json_response(200)

      assert %{"token" => alert_token} = response

      assert %{"name" => "adjusted"} =
               conn
               |> get(~p"/api/alerts/#{alert_token}")
               |> json_response(200)
    end

    test "patch alert returns no content and persists the update", %{conn: conn, user: user} do
      alert = insert(:alert, user: user, name: "initial")

      response =
        conn
        |> patch(~p"/api/alerts/#{alert.token}", %{name: "adjusted"})
        |> text_response(204)

      assert response == ""

      assert %{"name" => "adjusted"} =
               conn
               |> get(~p"/api/alerts/#{alert.token}")
               |> json_response(200)
    end

    test "update alert with invalid params", %{conn: conn, user: user} do
      alert = insert(:alert, user: user, cron: "0 0 1 * *")

      response =
        conn
        |> patch(~p"/api/alerts/#{alert.token}", %{cron: "not a cron expression"})
        |> json_response(422)

      assert %{"errors" => %{"cron" => _}} = response
    end

    test "backend IDs replace associations and preserve them when omitted", %{
      conn: conn,
      user: user,
      backend: backend
    } do
      alert = insert(:alert, user: user)
      other_backend = insert(:backend, user: user)

      response =
        conn
        |> put(~p"/api/alerts/#{alert.token}", %{backend_ids: [backend.id, other_backend.id]})
        |> json_response(200)

      assert MapSet.new(Enum.map(response["backends"], & &1["id"])) ==
               MapSet.new([backend.id, other_backend.id])

      response =
        conn
        |> put(~p"/api/alerts/#{alert.token}", %{backend_ids: [other_backend.id]})
        |> json_response(200)

      assert %{"backends" => [%{"id" => other_backend_id}]} = response
      assert other_backend_id == other_backend.id

      response =
        conn
        |> put(~p"/api/alerts/#{alert.token}", %{name: "renamed"})
        |> json_response(200)

      assert %{"backends" => [%{"id" => ^other_backend_id}]} = response

      assert %{"backends" => [%{"id" => ^other_backend_id}]} =
               conn
               |> get(~p"/api/alerts/#{alert.token}")
               |> json_response(200)

      assert %{"backends" => []} =
               conn
               |> put(~p"/api/alerts/#{alert.token}", %{backend_ids: []})
               |> json_response(200)

      assert %{"backends" => []} =
               conn
               |> get(~p"/api/alerts/#{alert.token}")
               |> json_response(200)
    end

    test "deduplicates backend IDs", %{conn: conn, user: user, backend: backend} do
      alert = insert(:alert, user: user)

      assert %{"backends" => [%{"id" => backend_id}]} =
               conn
               |> put(~p"/api/alerts/#{alert.token}", %{
                 backend_ids: [backend.id, backend.id]
               })
               |> json_response(200)

      assert backend_id == backend.id
    end

    test "rejects backend IDs that are not an array", %{conn: conn, backend: backend} do
      assert %{"error" => "backend_ids must be an array"} =
               conn
               |> post(~p"/api/alerts", %{
                 name: "my alert",
                 query: "select current_date() as date",
                 cron: "0 0 1 * *",
                 backend_ids: backend.id
               })
               |> json_response(400)
    end

    test "rejects non-positive and non-integer backend IDs", %{conn: conn, user: user} do
      alert = insert(:alert, user: user)

      for invalid_id <- [0, -1, 1.5, "1", %{}] do
        assert %{"error" => "backend_ids must contain positive integers"} =
                 conn
                 |> put(~p"/api/alerts/#{alert.token}", %{backend_ids: [invalid_id]})
                 |> json_response(400)
      end
    end

    test "rejects backend objects when updating", %{
      conn: conn,
      user: user,
      backend: backend
    } do
      alert = insert(:alert, user: user)

      assert %{"error" => "backends is read-only; use backend_ids"} =
               conn
               |> put(~p"/api/alerts/#{alert.token}", %{backends: [%{id: backend.id}]})
               |> json_response(400)
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

    test "team member can manage team-owned alerts", %{conn: conn} do
      member = insert(:user)
      team_user = insert(:team_user, email: member.email)
      owner = team_user.team.user
      managed_alert = insert(:alert, user: owner, name: "initial")
      deleted_alert = insert(:alert, user: owner)
      conn = add_access_token(conn, member, "private")

      response =
        conn
        |> get(~p"/api/alerts")
        |> json_response(200)

      assert MapSet.new(Enum.map(response, & &1["token"])) ==
               MapSet.new([managed_alert.token, deleted_alert.token])

      response =
        conn
        |> get(~p"/api/alerts/#{managed_alert.token}")
        |> json_response(200)

      assert response["token"] == managed_alert.token

      assert conn
             |> patch(~p"/api/alerts/#{managed_alert.token}", %{name: "updated"})
             |> text_response(204) == ""

      assert %{"name" => "updated"} =
               conn
               |> get(~p"/api/alerts/#{managed_alert.token}")
               |> json_response(200)

      assert conn
             |> delete(~p"/api/alerts/#{deleted_alert.token}")
             |> text_response(204) == ""
    end

    test "team member can attach a team-owned backend", %{conn: conn} do
      member = insert(:user)
      team_user = insert(:team_user, email: member.email)
      owner = team_user.team.user
      alert = insert(:alert, user: owner)
      backend = insert(:backend, user: owner)
      unrelated_backend = insert(:backend)
      conn = add_access_token(conn, member, "private")

      response =
        conn
        |> put(~p"/api/alerts/#{alert.token}", %{backend_ids: [backend.id]})
        |> json_response(200)

      assert %{"backends" => [%{"id" => backend_id}]} = response
      assert backend_id == backend.id

      assert %{"error" => "Not Found"} =
               conn
               |> put(~p"/api/alerts/#{alert.token}", %{backend_ids: [unrelated_backend.id]})
               |> json_response(404)
    end

    test "show alert with bad user", %{conn: conn, user: user} do
      alert = insert(:alert, user: user)

      conn
      |> add_access_token(insert(:user), "private")
      |> get(~p"/api/alerts/#{alert.token}")
      |> json_response(404)
    end

    test "delete alert", %{conn: conn, user: user} do
      alert = insert(:alert, user: user)

      response =
        conn
        |> delete(~p"/api/alerts/#{alert.token}")
        |> text_response(204)

      assert response == ""

      conn
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
