defmodule LogflareWeb.Api.AlertIssueTypeControllerTest do
  use LogflareWeb.ConnCase

  alias Logflare.AlertIssues.IssueTypes
  alias Logflare.AlertIssues.Silences
  alias Logflare.Alerting

  setup %{conn: conn} do
    insert(:plan, name: "Free")
    user = insert(:user)

    {:ok, _} =
      Alerting.create_alert_query(user, %{
        name: "Edge function errors",
        cron: "*/5 * * * *",
        query: "select 1",
        language: :pg_sql,
        rule_key: "logflare.edge-fn-5xx",
        managed: true
      })

    {:ok, conn: add_access_token(conn, user, "private"), user: user}
  end

  @filters %{"filters" => %{"identifiers.project" => "p1"}}

  describe "alert issue types" do
    test "GET lists alert issue types with their silence state", %{conn: conn, user: user} do
      {:ok, silence} =
        Silences.create(user, %{
          "rule_key" => "logflare.edge-fn-5xx",
          "matchers" => %{"project" => "p1"}
        })

      silence_id = silence.id

      assert [
               %{
                 "rule_key" => "logflare.edge-fn-5xx",
                 "managed" => true,
                 "silenced" => true,
                 "silence_id" => ^silence_id
               }
             ] =
               conn |> get(~p"/api/alerts/issue_types?#{@filters}") |> json_response(200)
    end

    test "POST creates a custom LQL alert issue type", %{conn: conn} do
      params = %{
        "name" => "Timeouts",
        "lql" => "f:edge m.status:>=500",
        "identifiers" => %{"project" => "p1"}
      }

      expect(IssueTypes, :create_custom, fn _user, ^params ->
        {:ok,
         %{
           rule_key: "custom.timeouts",
           name: "Timeouts",
           description: nil,
           category: :health,
           severity: :warning,
           managed: false,
           language: :lql,
           lql: "f:edge m.status:>=500",
           group_by: [],
           silenced: false,
           silence_id: nil
         }}
      end)

      assert %{"rule_key" => "custom.timeouts", "lql" => "f:edge m.status:>=500"} =
               conn |> post(~p"/api/alerts/issue_types", params) |> json_response(201)
    end

    test "POST returns 422 with validation errors", %{conn: conn} do
      assert %{"errors" => %{"lql" => [_]}} =
               conn
               |> post(~p"/api/alerts/issue_types", %{
                 "name" => "Bad",
                 "lql" => "m.status:500",
                 "identifiers" => %{"project" => "p1"}
               })
               |> json_response(422)
    end

    test "DELETE removes custom types and refuses managed ones", %{conn: conn} do
      stub(IssueTypes, :delete_custom, fn
        _user, "custom.timeouts" -> :ok
        _user, "logflare.edge-fn-5xx" -> {:error, :managed}
      end)

      assert conn |> delete(~p"/api/alerts/issue_types/custom.timeouts") |> response(204)

      assert %{"error" => _} =
               conn
               |> delete(~p"/api/alerts/issue_types/logflare.edge-fn-5xx")
               |> json_response(422)
    end

    test "GET requires filters", %{conn: conn} do
      conn |> get(~p"/api/alerts/issue_types") |> json_response(400)
    end
  end

  describe "silences" do
    test "POST creates and DELETE removes a silence", %{conn: conn, user: user} do
      assert %{"id" => id, "matchers" => %{"project" => "p1"}, "created_by" => "ops"} =
               conn
               |> post(~p"/api/alerts/silences", %{
                 "rule_key" => "logflare.edge-fn-5xx",
                 "matchers" => %{"project" => "p1"},
                 "created_by" => "ops"
               })
               |> json_response(201)

      assert [%{"id" => ^id}] =
               conn |> get(~p"/api/alerts/silences?#{@filters}") |> json_response(200)

      assert conn |> delete(~p"/api/alerts/silences/#{id}") |> response(204)
      assert [] == Silences.list(user)
    end

    test "POST rejects invalid matchers", %{conn: conn} do
      conn
      |> post(~p"/api/alerts/silences", %{"matchers" => %{"Bad Key" => "x"}})
      |> json_response(422)
    end

    test "DELETE returns 404 for another user's silence", %{conn: conn} do
      {:ok, silence} = Silences.create(insert(:user), %{"matchers" => %{"project" => "p1"}})

      conn |> delete(~p"/api/alerts/silences/#{silence.id}") |> json_response(404)
      conn |> delete(~p"/api/alerts/silences/nope") |> json_response(404)
    end
  end
end
