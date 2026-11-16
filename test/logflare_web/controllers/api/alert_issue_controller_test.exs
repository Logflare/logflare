defmodule LogflareWeb.Api.AlertIssueControllerTest do
  use LogflareWeb.ConnCase

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

  test "existing alert routes still resolve by token", %{conn: conn, user: user} do
    alert = insert(:alert, user: user)
    assert %{"id" => id} = conn |> get(~p"/api/alerts/#{alert.token}") |> json_response(200)
    assert id == alert.id
  end
end
