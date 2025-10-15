defmodule LogflareWeb.Api.TeamControllerTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  alias Logflare.TeamUsers

  setup do
    insert(:plan)
    user = insert(:user)
    main_team = insert(:team, user: user)

    another_user_team_with_main_user = insert(:team, user: insert(:user))
    insert(:team_user, team: another_user_team_with_main_user, provider_uid: user.provider_uid)

    _non_relevant_to_main_user =
      insert(:team_user, team: main_team, provider_uid: insert(:user).provider_uid)

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
        |> get(~p"/api/teams")
        |> json_response(200)
        |> assert_schema("TeamListResponse")

      assert Enum.any?(response, fn %{token: token} -> main_team_token == token end)
      assert Enum.any?(response, fn %{token: token} -> non_owner_team_token == token end)
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
        |> assert_schema("Team")

      assert response.token == token
    end

    test "returns a single team given user and team token where his not an owner but a member", %{
      conn: conn,
      user: user,
      non_owner_team: non_owner_team
    } do
      response =
        conn
        |> add_access_token(user, "private")
        |> get(~p"/api/teams/#{non_owner_team.token}")
        |> json_response(200)
        |> assert_schema("Team")

      assert response.token == non_owner_team.token
      assert response.name == non_owner_team.name
    end

    test "returns not found if doesn't own the team or isn't part of it", %{
      conn: conn,
      main_team: main_team,
      non_owner_team: non_owner_team
    } do
      invalid_user = insert(:user)

      conn
      |> add_access_token(invalid_user, "private")
      |> get(~p"/api/teams/#{main_team.token}")
      |> json_response(404)
      |> assert_schema("NotFoundResponse")

      conn
      |> add_access_token(invalid_user, "private")
      |> get(~p"/api/teams/#{non_owner_team.token}")
      |> json_response(404)
      |> assert_schema("NotFoundResponse")
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
        |> assert_schema("Team")

      assert response.name == name
    end

    test "returns 422 on bad arguments", %{conn: conn, user: user} do
      response =
        conn
        |> add_access_token(user, "private")
        |> post(~p"/api/teams", %{name: 123})
        |> json_response(422)
        |> assert_schema("UnprocessableEntityResponse")

      assert response == %{errors: %{"name" => ["is invalid"]}}
    end

    test "returns 422 on missing arguments", %{conn: conn, user: user} do
      response =
        conn
        |> add_access_token(user, "private")
        |> post(~p"/api/teams")
        |> json_response(422)
        |> assert_schema("UnprocessableEntityResponse")

      assert response == %{errors: %{"name" => ["can't be blank"]}}
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
        |> assert_schema("AcceptedResponse")

      assert response == ""

      another_name = TestUtils.random_string()

      response =
        conn
        |> add_access_token(user, "private")
        |> put(~p"/api/teams/#{main_team.token}", %{name: another_name})
        |> json_response(201)
        |> assert_schema("Team")

      assert response.token == main_team.token
      assert response.name == another_name
    end

    test "returns not found if doesn't own the team", %{conn: conn, main_team: main_team} do
      invalid_user = insert(:user)

      conn
      |> add_access_token(invalid_user, "private")
      |> patch(~p"/api/teams/#{main_team.token}", %{name: TestUtils.random_string()})
      |> json_response(404)
      |> assert_schema("NotFoundResponse")
    end

    test "returns 422 on bad arguments", %{conn: conn, user: user, main_team: main_team} do
      response =
        conn
        |> add_access_token(user, "private")
        |> patch(~p"/api/teams/#{main_team.token}", %{name: 123})
        |> json_response(422)
        |> assert_schema("UnprocessableEntityResponse")

      assert response == %{errors: %{"name" => ["is invalid"]}}
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
             |> response(204)
             |> assert_schema("AcceptedResponse") == ""
    end

    test "returns not found if doesn't own the team", %{conn: conn, main_team: main_team} do
      invalid_user = insert(:user)

      assert conn
             |> add_access_token(invalid_user, "private")
             |> delete(~p"/api/teams/#{main_team.token}")
             |> json_response(404)
             |> assert_schema("NotFoundResponse") == %{error: "Not Found"}
    end
  end

  describe "add_member/2" do
    test "adds an existing user to a team", %{
      conn: conn,
      user: user,
      main_team: main_team
    } do
      new_member = insert(:user)

      assert conn
             |> add_access_token(user, "private")
             |> post(~p"/api/teams/#{main_team.token}/members", %{email: new_member.email})
             |> response(204)
             |> assert_schema("AcceptedResponse") == ""

      team_users = TeamUsers.list_team_users_by(team_id: main_team.id)

      assert Enum.any?(team_users, fn tu ->
               tu.email == String.downcase(new_member.email)
             end)
    end

    test "creates a new team member when adding with non-existent email", %{
      conn: conn,
      user: user,
      main_team: main_team
    } do
      new_email = "newuser@example.com"

      assert conn
             |> add_access_token(user, "private")
             |> post(~p"/api/teams/#{main_team.token}/members", %{email: new_email})
             |> response(204)
             |> assert_schema("AcceptedResponse") == ""

      team_users = TeamUsers.list_team_users_by(team_id: main_team.id)
      assert Enum.any?(team_users, fn tu -> tu.email == String.downcase(new_email) end)
    end

    test "returns not found if doesn't own the team", %{conn: conn, main_team: main_team} do
      invalid_user = insert(:user)
      new_member = insert(:user)

      assert conn
             |> add_access_token(invalid_user, "private")
             |> post(~p"/api/teams/#{main_team.token}/members", %{email: new_member.email})
             |> json_response(404)
             |> assert_schema("NotFoundResponse") == %{error: "Not Found"}
    end
  end

  describe "remove_member/2" do
    test "removes a member from a team", %{
      conn: conn,
      user: user,
      main_team: main_team
    } do
      member_to_remove = insert(:user)
      insert(:team_user, team: main_team, email: member_to_remove.email)

      assert conn
             |> add_access_token(user, "private")
             |> delete(~p"/api/teams/#{main_team.token}/members/#{member_to_remove.email}")
             |> response(204)
             |> assert_schema("AcceptedResponse") == ""

      team_users = TeamUsers.list_team_users_by(team_id: main_team.id)
      refute Enum.any?(team_users, fn tu -> tu.email == member_to_remove.email end)
    end

    test "returns not found if doesn't own the team", %{conn: conn, main_team: main_team} do
      invalid_user = insert(:user)
      member = insert(:user)
      insert(:team_user, team: main_team, email: member.email)

      assert conn
             |> add_access_token(invalid_user, "private")
             |> delete(~p"/api/teams/#{main_team.token}/members/#{member.email}")
             |> json_response(404)
             |> assert_schema("NotFoundResponse") == %{error: "Not Found"}
    end

    test "returns not found if team member doesn't exist", %{
      conn: conn,
      user: user,
      main_team: main_team
    } do
      non_existent_email = "nonexistent@example.com"

      assert conn
             |> add_access_token(user, "private")
             |> delete(~p"/api/teams/#{main_team.token}/members/#{non_existent_email}")
             |> json_response(404)
             |> assert_schema("NotFoundResponse") == %{error: "Not Found"}
    end
  end
end
