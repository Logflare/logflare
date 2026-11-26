defmodule LogflareWeb.Api.TeamControllerTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  setup do
    insert(:plan)
    user = insert(:user)
    main_team = insert(:team, user: user)

    another_user_team_with_main_user = insert(:team, user: insert(:user))

    insert(:team_user,
      team: another_user_team_with_main_user,
      email: user.email,
      provider_uid: user.provider_uid
    )

    _non_relevant_to_main_user =
      insert(:team_user, team: main_team, provider_uid: insert(:user).provider_uid)

    {:ok, user: user, main_team: main_team, non_owner_team: another_user_team_with_main_user}
  end

  describe "index/2" do
    test "returns only the team that the key's account owns", %{
      conn: conn,
      user: user,
      main_team: %{token: main_team_token}
    } do
      assert [%{"token" => token}] =
               conn
               |> add_access_token(user, "private")
               |> get(~p"/api/teams")
               |> json_response(200)

      assert token == main_team_token
    end

    test "returns an empty list when the key's account owns no team", %{conn: conn} do
      assert [] =
               conn
               |> add_access_token(insert(:user), "private")
               |> get(~p"/api/teams")
               |> json_response(200)
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
        |> get(~p"/api/teams/#{token}")
        |> json_response(200)

      assert response["token"] == token
    end

    test "returns the owned team given the token of one of its team users", %{
      conn: conn,
      user: user,
      main_team: main_team
    } do
      [team_user] = Logflare.TeamUsers.list_team_users_by(team_id: main_team.id)

      assert %{"token" => token} =
               conn
               |> add_access_token(user, "private")
               |> get(~p"/api/teams/#{team_user.token}")
               |> json_response(200)

      assert token == main_team.token
    end

    test "returns not found given a team-user token of a team that the key's account is only a member of",
         %{conn: conn, user: user, non_owner_team: non_owner_team} do
      [team_user] = Logflare.TeamUsers.list_team_users_by(team_id: non_owner_team.id)

      conn
      |> add_access_token(user, "private")
      |> get(~p"/api/teams/#{team_user.token}")
      |> json_response(404)
    end

    test "returns not found for a team that the key's account is only a member of", %{
      conn: conn,
      user: user,
      non_owner_team: non_owner_team
    } do
      conn
      |> add_access_token(user, "private")
      |> get(~p"/api/teams/#{non_owner_team.token}")
      |> json_response(404)
    end

    test "returns not found if doesn't own the team or isn't part of it", %{
      conn: conn,
      main_team: main_team,
      non_owner_team: non_owner_team
    } do
      invalid_user = insert(:user)

      conn =
        conn
        |> add_access_token(invalid_user, "private")
        |> get(~p"/api/teams/#{main_team.token}")

      assert ["application/json; charset=utf-8"] = get_resp_header(conn, "content-type")

      conn
      |> json_response(404)

      conn
      |> recycle()
      |> add_access_token(invalid_user, "private")
      |> get(~p"/api/teams/#{non_owner_team.token}")
      |> json_response(404)
    end
  end

  describe "create/2" do
    test "creates a new team for an authenticated user", %{conn: conn} do
      user = insert(:user)
      name = TestUtils.random_string()

      response =
        conn
        |> add_access_token(user, "private")
        |> post(~p"/api/teams", %{name: name})
        |> json_response(201)

      assert response["name"] == name
    end

    test "returns 422 on bad arguments", %{conn: conn, user: user} do
      response =
        conn
        |> add_access_token(user, "private")
        |> post(~p"/api/teams", %{name: 123})
        |> json_response(422)

      assert response == %{"errors" => %{"name" => ["is invalid"]}}
    end

    test "returns 422 on missing arguments", %{conn: conn, user: user} do
      response =
        conn
        |> add_access_token(user, "private")
        |> post(~p"/api/teams")
        |> json_response(422)

      assert response == %{"errors" => %{"name" => ["can't be blank"]}}
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
        |> patch(~p"/api/teams/#{main_team.token}", %{name: name})
        |> response(204)

      assert response == ""

      another_name = TestUtils.random_string()

      response =
        conn
        |> add_access_token(user, "private")
        |> put(~p"/api/teams/#{main_team.token}", %{name: another_name})
        |> json_response(201)

      assert response["token"] == main_team.token
      assert response["name"] == another_name
    end

    test "returns not found if doesn't own the team", %{conn: conn, main_team: main_team} do
      invalid_user = insert(:user)

      conn
      |> add_access_token(invalid_user, "private")
      |> patch(~p"/api/teams/#{main_team.token}", %{name: TestUtils.random_string()})
      |> json_response(404)
    end

    test "returns 422 on bad arguments", %{conn: conn, user: user, main_team: main_team} do
      response =
        conn
        |> add_access_token(user, "private")
        |> patch(~p"/api/teams/#{main_team.token}", %{name: 123})
        |> json_response(422)

      assert response == %{"errors" => %{"name" => ["is invalid"]}}
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
             |> delete(~p"/api/teams/#{main_team.token}")
             |> response(204) == ""
    end

    test "returns not found if doesn't own the team", %{conn: conn, main_team: main_team} do
      invalid_user = insert(:user)

      assert conn
             |> add_access_token(invalid_user, "private")
             |> delete(~p"/api/teams/#{main_team.token}")
             |> json_response(404) == %{"error" => "Not Found"}
    end
  end
end
