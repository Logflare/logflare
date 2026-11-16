defmodule LogflareWeb.Api.AlertIssueControllerTest do
  use LogflareWeb.ConnCase

  alias Logflare.AlertIssues
  alias Logflare.AlertIssues.LogEvents
  alias Logflare.AlertIssues.Queries

  @issue %{"alert_issue_id" => "a1", "status" => "open", "identifiers" => %{"project" => "p1"}}

  setup %{conn: conn} do
    insert(:plan, name: "Free")
    user = insert(:user)
    {:ok, conn: add_access_token(conn, user, "private"), user: user}
  end

  describe "GET /api/alerts/issues" do
    test "lists alert issues scoped by identifier filters", %{conn: conn, user: user} do
      user_id = user.id

      expect(Queries, :list, fn %{id: ^user_id}, opts ->
        assert opts[:filters] == %{"project" => "p1"}
        assert opts[:statuses] == ["open", "acknowledged"]
        assert opts[:since] == ~U[2026-11-10 00:00:00Z]
        {:ok, [@issue]}
      end)

      response =
        conn
        |> get(
          ~p"/api/alerts/issues?#{%{"filters" => %{"identifiers.project" => "p1"}, "status" => "open,acknowledged", "since" => "2026-11-10T00:00:00Z"}}"
        )
        |> json_response(200)

      assert response == [@issue]
    end

    test "requires at least one identifier filter", %{conn: conn} do
      reject(Queries, :list, 2)

      assert %{"error" => "filters[identifiers.<key>] is required"} =
               conn |> get(~p"/api/alerts/issues") |> json_response(400)
    end

    test "rejects filters outside identifiers", %{conn: conn} do
      reject(Queries, :list, 2)

      assert %{"error" => _} =
               conn
               |> get(~p"/api/alerts/issues?#{%{"filters" => %{"status" => "open"}}}")
               |> json_response(400)
    end

    test "rejects an invalid since", %{conn: conn} do
      reject(Queries, :list, 2)

      assert %{"error" => _} =
               conn
               |> get(
                 ~p"/api/alerts/issues?#{%{"filters" => %{"identifiers.project" => "p1"}, "since" => "yesterday"}}"
               )
               |> json_response(400)
    end

    test "returns backend query errors as 500 with a message", %{conn: conn} do
      stub(Queries, :list, fn _user, _opts ->
        {:error,
         %Logflare.Backends.QueryError{
           kind: :backend_error,
           raw_error: :boom,
           backend: __MODULE__,
           description: "relation does not exist"
         }}
      end)

      assert %{"error" => "relation does not exist"} =
               conn
               |> get(~p"/api/alerts/issues?#{%{"filters" => %{"identifiers.project" => "p1"}}}")
               |> json_response(500)
    end

    test "maps an invalid filter key to 400", %{conn: conn} do
      stub(Queries, :list, fn _user, _opts -> {:error, {:invalid_filter_key, "Bad"}} end)

      assert %{"error" => "invalid filter key: Bad"} =
               conn
               |> get(~p"/api/alerts/issues?#{%{"filters" => %{"identifiers.Bad" => "x"}}}")
               |> json_response(400)
    end
  end

  describe "GET /api/alerts/issues/:alert_issue_id" do
    test "returns the alert issue", %{conn: conn} do
      expect(Queries, :get, fn _user, "a1", opts ->
        assert opts[:filters] == %{"project" => "p1"}
        {:ok, @issue}
      end)

      assert conn
             |> get(~p"/api/alerts/issues/a1?#{%{"filters" => %{"identifiers.project" => "p1"}}}")
             |> json_response(200) == @issue
    end

    test "returns 404 when the alert issue is outside the filters", %{conn: conn} do
      stub(Queries, :get, fn _user, _id, _opts -> {:error, :not_found} end)

      assert conn
             |> get(~p"/api/alerts/issues/a1?#{%{"filters" => %{"identifiers.project" => "p2"}}}")
             |> json_response(404)
    end
  end

  describe "POST /api/alerts/issues/:alert_issue_id/actions" do
    @filters %{"identifiers.project" => "p1"}

    test "applies an action as the given actor", %{conn: conn, user: user} do
      user_id = user.id

      expect(AlertIssues, :act, fn %{id: ^user_id}, "a1", :snooze, opts ->
        assert opts[:filters] == %{"project" => "p1"}
        assert opts[:actor] == "dev@example.com"
        assert opts[:until] == ~U[2026-11-16 13:00:00Z]
        {:ok, %{@issue | "status" => "snoozed"}}
      end)

      assert %{"status" => "snoozed"} =
               conn
               |> post(~p"/api/alerts/issues/a1/actions", %{
                 "filters" => @filters,
                 "action" => "snooze",
                 "actor" => "dev@example.com",
                 "until" => "2026-11-16T13:00:00Z"
               })
               |> json_response(200)
    end

    test "defaults the actor to the API user", %{conn: conn, user: user} do
      expect(AlertIssues, :act, fn _user, "a1", :acknowledge, opts ->
        assert opts[:actor] == user.email
        {:ok, @issue}
      end)

      conn
      |> post(~p"/api/alerts/issues/a1/actions", %{
        "filters" => @filters,
        "action" => "acknowledge"
      })
      |> json_response(200)
    end

    test "rejects unknown actions", %{conn: conn} do
      reject(AlertIssues, :act, 4)

      assert %{"error" => _} =
               conn
               |> post(~p"/api/alerts/issues/a1/actions", %{
                 "filters" => @filters,
                 "action" => "delete"
               })
               |> json_response(400)
    end

    test "maps errors to status codes", %{conn: conn} do
      for {error, status} <- [
            {:invalid_transition, 409},
            {:until_required, 400},
            {:not_found, 404}
          ] do
        stub(AlertIssues, :act, fn _user, _id, _action, _opts -> {:error, error} end)

        assert conn
               |> post(~p"/api/alerts/issues/a1/actions", %{
                 "filters" => @filters,
                 "action" => "resolve"
               })
               |> json_response(status)
      end
    end

    test "requires identifier filters", %{conn: conn} do
      reject(AlertIssues, :act, 4)

      assert conn
             |> post(~p"/api/alerts/issues/a1/actions", %{"action" => "resolve"})
             |> json_response(400)
    end
  end

  describe "reads" do
    test "GET passes the reader through to list", %{conn: conn} do
      expect(Queries, :list, fn _user, opts ->
        assert opts[:reader] == "user_1"
        {:ok, [Map.put(@issue, "unread", true)]}
      end)

      assert [%{"unread" => true}] =
               conn
               |> get(
                 ~p"/api/alerts/issues?#{%{"filters" => %{"identifiers.project" => "p1"}, "reader" => "user_1"}}"
               )
               |> json_response(200)
    end

    test "POST /api/alerts/issues/reads marks alert issues read", %{conn: conn} do
      expect(AlertIssues, :mark_read, fn _user, ["a1", "a2"], "user_1", true, opts ->
        assert opts[:filters] == %{"project" => "p1"}
        {:ok, 2}
      end)

      assert %{"count" => 2} =
               conn
               |> post(~p"/api/alerts/issues/reads", %{
                 "filters" => %{"identifiers.project" => "p1"},
                 "alert_issue_ids" => ["a1", "a2"],
                 "reader" => "user_1",
                 "read" => true
               })
               |> json_response(200)
    end

    test "POST /api/alerts/issues/reads validates the body", %{conn: conn} do
      reject(AlertIssues, :mark_read, 5)

      assert conn
             |> post(~p"/api/alerts/issues/reads", %{
               "filters" => %{"identifiers.project" => "p1"},
               "alert_issue_ids" => [],
               "reader" => "user_1"
             })
             |> json_response(400)
    end
  end

  test "existing alert routes still resolve by token", %{conn: conn, user: user} do
    alert = insert(:alert, user: user)
    assert %{"id" => id} = conn |> get(~p"/api/alerts/#{alert.token}") |> json_response(200)
    assert id == alert.id
  end

  describe "alert issue comments" do
    @filters %{"filters" => %{"identifiers.project" => "p1"}}
    @comment %{
      "comment_id" => "c1",
      "alert_issue_id" => "a1",
      "author" => "ops",
      "body" => "Looking into it",
      "created_at" => "2026-11-16T12:00:00Z"
    }

    test "GET lists comments within the filters", %{conn: conn} do
      expect(Queries, :comments, fn _user, "a1", opts ->
        assert opts[:filters] == %{"project" => "p1"}
        {:ok, [@comment]}
      end)

      assert [@comment] ==
               conn
               |> get(~p"/api/alerts/issues/a1/comments?#{@filters}")
               |> json_response(200)
    end

    test "GET returns 404 for an alert issue outside the filters", %{conn: conn} do
      stub(Queries, :comments, fn _user, _id, _opts -> {:error, :not_found} end)

      conn
      |> get(~p"/api/alerts/issues/zzz/comments?#{@filters}")
      |> json_response(404)
    end

    test "POST creates a comment", %{conn: conn} do
      expect(AlertIssues, :comment, fn _user, "a1", "Looking into it", opts ->
        assert opts[:filters] == %{"project" => "p1"}
        assert opts[:author] == "ops"
        {:ok, @comment}
      end)

      assert @comment ==
               conn
               |> post(
                 ~p"/api/alerts/issues/a1/comments?#{@filters}",
                 %{"body" => "Looking into it", "author" => "ops"}
               )
               |> json_response(201)
    end

    test "POST rejects an invalid body with 422", %{conn: conn} do
      stub(AlertIssues, :comment, fn _user, _id, _body, _opts -> {:error, :invalid_body} end)

      assert %{"error" => _} =
               conn
               |> post(~p"/api/alerts/issues/a1/comments?#{@filters}", %{"body" => " "})
               |> json_response(422)
    end

    test "POST requires filters", %{conn: conn} do
      reject(AlertIssues, :comment, 4)

      conn
      |> post(~p"/api/alerts/issues/a1/comments", %{"body" => "hi"})
      |> json_response(400)
    end
  end

  describe "log previews" do
    @preview %{
      lql: ~s(f:edge_logs_demo m.function_id:"f1"),
      source: "edge_logs_demo",
      from: ~U[2026-11-16 11:00:00Z],
      to: ~U[2026-11-16 12:00:00Z],
      total: 1,
      buckets: [%{timestamp: ~U[2026-11-16 11:30:00Z], count: 1}],
      events: [
        %{
          id: "e1",
          timestamp: ~U[2026-11-16 11:30:00Z],
          event_message: "boom",
          body: %{"metadata" => %{"function_id" => "f1"}}
        }
      ]
    }

    test "GET events previews the alert issue's log events", %{conn: conn} do
      expect(LogEvents, :for_alert_issue, fn _user, "a1", opts ->
        assert opts[:filters] == %{"project" => "p1"}
        assert opts[:from] == ~U[2026-11-16 11:00:00Z]
        assert opts[:to] == nil
        {:ok, @preview}
      end)

      assert %{"total" => 1, "events" => [%{"id" => "e1"}], "buckets" => [%{"count" => 1}]} =
               conn
               |> get(
                 ~p"/api/alerts/issues/a1/events?#{%{"filters" => %{"identifiers.project" => "p1"}, "from" => "2026-11-16T11:00:00Z"}}"
               )
               |> json_response(200)
    end

    test "GET events returns 422 when the rule has no preview", %{conn: conn} do
      stub(LogEvents, :for_alert_issue, fn _user, _id, _opts -> {:error, :no_preview} end)

      assert %{"error" => "this alert issue type has no log preview"} =
               conn
               |> get(
                 ~p"/api/alerts/issues/a1/events?#{%{"filters" => %{"identifiers.project" => "p1"}}}"
               )
               |> json_response(422)
    end

    test "GET events returns 404 outside the filters", %{conn: conn} do
      stub(LogEvents, :for_alert_issue, fn _user, _id, _opts -> {:error, :not_found} end)

      conn
      |> get(~p"/api/alerts/issues/a1/events?#{%{"filters" => %{"identifiers.project" => "p1"}}}")
      |> json_response(404)
    end

    test "POST preview runs an LQL filter", %{conn: conn} do
      expect(LogEvents, :preview, fn _user, "f:edge_logs_demo error", _range ->
        {:ok, @preview}
      end)

      assert %{"source" => "edge_logs_demo"} =
               conn
               |> post(~p"/api/alerts/preview", %{"lql" => "f:edge_logs_demo error"})
               |> json_response(200)
    end

    test "POST preview returns 422 for LQL without a source", %{conn: conn} do
      stub(LogEvents, :preview, fn _user, _lql, _range -> {:error, :source_required} end)

      assert %{"error" => "lql must name a source with f:<source>"} =
               conn
               |> post(~p"/api/alerts/preview", %{"lql" => "error"})
               |> json_response(422)
    end
  end
end
