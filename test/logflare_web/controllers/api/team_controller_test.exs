defmodule LogflareWeb.Api.TeamControllerTest do
  use LogflareWeb.ConnCase

  import Logflare.Factory

  alias Logflare.Sources.Counters

  setup do
    insert(:plan, name: "Free")
    user = insert(:user)
    main_team = insert(:team, user: user)

    another_user_team_with_main_user = insert(:team, user: insert(:user))
    insert(:team_user, team: another_user_team_with_main_user, email: user.email)

    _non_relevant_to_main_user = insert(:team_user, team: main_team, email: insert(:user).email)

    Counters.start_link()

    {:ok, user: user, main_team: main_team, non_owner_team: another_user_team_with_main_user}
  end

  describe "index/2" do
    test "returns list of teams for given user", %{
      conn: conn,
      user: user,
      main_team: %{token: main_team_token},
      non_owner_team: %{token: non_owner_team_token}
    } do
      response =
        conn
        |> add_access_token(user, "private")
        |> get("/api/teams")
        |> json_response(200)

      assert Enum.any?(response, fn %{"token" => token} -> main_team_token == token end)
      assert Enum.any?(response, fn %{"token" => token} -> non_owner_team_token == token end)
    end
  end

  describe "show/2" do
    test "returns a single team given user and team token", %{
      conn: conn,
      user: user,
      main_team: %{token: token}
    } do
      response =
        conn
        |> add_access_token(user, "private")
        |> get("/api/teams/#{token}")
        |> json_response(200)

      assert response["token"] == token
    end

    test "returns a single team given user and team token where his not an owner but a member", %{
      conn: conn,
      user: user,
      non_owner_team: non_owner_team
    } do
      response =
        conn
        |> add_access_token(user, "private")
        |> get("/api/teams/#{non_owner_team.token}")
        |> json_response(200)

      assert response["name"] == non_owner_team.name
    end

    test "returns not found if doesn't own the team or isn't part of it", %{
      conn: conn,
      main_team: main_team,
      non_owner_team: non_owner_team
    } do
      invalid_user = insert(:user)

      conn
      |> add_access_token(invalid_user, "private")
      |> get("/api/teams/#{main_team.token}")
      |> response(404)

      conn
      |> add_access_token(invalid_user, "private")
      |> get("/api/teams/#{non_owner_team.token}")
      |> response(404)
    end
  end

  describe "create/2" do
    test "creates a new team for an authenticated user", %{conn: conn} do
      user = insert(:user)
      name = TestUtils.random_string()

      response =
        conn
        |> add_access_token(user, "private")
        |> post("/api/teams", %{name: name})
        |> json_response(201)

      assert response["name"] == name
    end

    test "returns 422 on bad arguments", %{conn: conn, user: user} do
      resp =
        conn
        |> add_access_token(user, "private")
        |> post("/api/teams", %{name: 123})
        |> json_response(422)

      assert resp == %{"errors" => %{"name" => ["is invalid"]}}
    end

    test "returns 422 on missing arguments", %{conn: conn, user: user} do
      resp =
        conn
        |> add_access_token(user, "private")
        |> post("/api/teams")
        |> json_response(422)

      assert resp == %{"errors" => %{"name" => ["can't be blank"]}}
    end
  end

  describe "update/2" do
    test "updates an existing team from a user", %{
      conn: conn,
      user: user,
      main_team: main_team
    } do
      name = TestUtils.random_string()

      response =
        conn
        |> add_access_token(user, "private")
        |> patch("/api/teams/#{main_team.token}", %{name: name})
        |> json_response(204)

      assert response["name"] == name
    end

    test "returns not found if doesn't own the team", %{conn: conn, main_team: main_team} do
      invalid_user = insert(:user)

      conn
      |> add_access_token(invalid_user, "private")
      |> patch("/api/teams/#{main_team.token}", %{name: TestUtils.random_string()})
      |> response(404)
    end

    test "returns 422 on bad arguments", %{conn: conn, user: user, main_team: main_team} do
      resp =
        conn
        |> add_access_token(user, "private")
        |> patch("/api/teams/#{main_team.token}", %{name: 123})
        |> json_response(422)

      assert resp == %{"errors" => %{"name" => ["is invalid"]}}
    end
  end

  describe "delete/2" do
    test "deletes an existing team from a user", %{
      conn: conn,
      user: user,
      main_team: main_team
    } do
      assert conn
             |> add_access_token(user, "private")
             |> delete("/api/teams/#{main_team.token}")
             |> response(204)
    end

    test "returns not found if doesn't own the team", %{conn: conn, main_team: main_team} do
      invalid_user = insert(:user)

      assert conn
             |> add_access_token(invalid_user, "private")
             |> delete("/api/teams/#{main_team.token}")
             |> response(404)
    end
  end
end
