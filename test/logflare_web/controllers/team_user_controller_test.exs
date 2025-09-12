defmodule LogflareWeb.TeamUserControllerTest do
  use LogflareWeb.ConnCase, async: true

  setup do
    insert(:plan)
    :ok
  end

  test "user can edit their profile", %{conn: conn} do
    user = insert(:user)
    team = insert(:team, user: user)
    team_user = insert(:team_user, team: team, email: user.email)

    new_name = "Avengers"
    new_email = "tony.stark@avengers.com"
    new_phone = "+1 (555) 123-4567"

    conn
    |> login_user(user)
    |> put_session(:team_user_id, team_user.id)
    |> visit(~p"/profile/edit")
    |> assert_has("h5", text: "Profile Preferences", exact: true)
    |> fill_in("Name", with: new_name)
    |> fill_in("Preferred email", with: new_email)
    |> fill_in("Phone number", with: new_phone)
    |> click_button("Update preferences")
    |> assert_has("input#team_user_name", value: new_name)
    |> assert_has("input#team_user_email_preferred", value: new_email)
    |> assert_has("input#team_user_phone", value: new_phone)
    |> assert_path(~p"/profile/edit")

    team_user = Logflare.Repo.reload!(team_user)
    assert team_user.name == new_name
    assert team_user.email_preferred == new_email
    assert team_user.phone == new_phone
  end

  test "not authenticated user is redirected to login page", %{conn: conn} do
    conn
    |> visit(~p"/profile/edit")
    |> assert_path(~p"/auth/login")
  end

  test "user can leave their team", %{conn: conn} do
    user = insert(:user)
    team = insert(:team, user: user)
    team_user = insert(:team_user, team: team, email: user.email)

    conn
    |> login_user(user)
    |> put_session(:team_user_id, team_user.id)
    |> visit(~p"/profile/edit")
    |> assert_has("h5", text: "Profile Preferences", exact: true)
    |> assert_has("a", text: "Leave now")

    expect(
      GoogleApi.CloudResourceManager.V1.Api.Projects,
      :cloudresourcemanager_projects_set_iam_policy,
      fn _, _project_number, [body: _body] -> {:ok, ""} end
    )

    conn =
      conn
      |> login_user(user)
      |> put_session(:team_user_id, team_user.id)
      |> delete(~p"/profile")

    assert redirected_to(conn, 302) =~ "/login"
    assert Phoenix.Flash.get(conn.assigns.flash, :info) == "Profile deleted!"
    refute Logflare.TeamUsers.get_team_user(team_user.id)
  end

  test "user switching between teams with team_user_id", %{conn: conn} do
    user = insert(:user)
    other_user = insert(:user)
    team1 = insert(:team, user: user)
    team_user1 = insert(:team_user, team: team1, email: user.email)
    team2 = insert(:team, user: other_user)
    team_user2 = insert(:team_user, team: team2, email: user.email)

    conn =
      conn
      |> login_user(user)
      |> put_session(:team_user_id, team_user1.id)
      |> get(~p"/profile/switch", %{"user_id" => user.id, "team_user_id" => team_user2.id})

    assert redirected_to(conn, 302) =~ "/dashboard"
    assert Phoenix.Flash.get(conn.assigns.flash, :info) == "Welcome to this Logflare team!"
    assert get_session(conn, :user_id) == to_string(user.id)
    assert get_session(conn, :team_user_id) == to_string(team_user2.id)
  end

  test "user switching to personal account", %{conn: conn} do
    user = insert(:user)
    team = insert(:team, user: user)
    team_user = insert(:team_user, team: team, email: user.email)

    conn =
      conn
      |> login_user(user)
      |> put_session(:team_user_id, team_user.id)
      |> get(~p"/profile/switch", %{"user_id" => user.id})

    assert redirected_to(conn, 302) == ~p"/dashboard"
    assert Phoenix.Flash.get(conn.assigns.flash, :info) == "Welcome to this Logflare team!"
    assert get_session(conn, :user_id) == to_string(user.id)
    assert get_session(conn, :team_user_id) == nil
  end

  test "user can switch from personal (no team_user_id) to team account", %{conn: conn} do
    user = insert(:user)
    team = insert(:team, user: user)
    team_user = insert(:team_user, team: team, email: user.email)

    conn =
      conn
      |> login_user(user)
      |> get(~p"/profile/switch", %{"user_id" => user.id, "team_user_id" => team_user.id})

    assert redirected_to(conn, 302) == ~p"/dashboard"
    assert Phoenix.Flash.get(conn.assigns.flash, :info) == "Welcome to this Logflare team!"
    assert get_session(conn, :user_id) == to_string(user.id)
    assert get_session(conn, :team_user_id) == to_string(team_user.id)
  end

  test "when user switches team is redirected using redirect_to param", %{conn: conn} do
    user = insert(:user)
    team = insert(:team, user: user)
    team_user = insert(:team_user, team: team, email: user.email)

    conn =
      conn
      |> login_user(user)
      |> put_session(:team_user_id, team_user.id)
      |> get(~p"/profile/switch", %{
        "user_id" => user.id,
        "team_user_id" => team_user.id,
        "redirect_to" => "/sources"
      })

    assert redirected_to(conn, 302) =~ ~p"/sources"
    assert Phoenix.Flash.get(conn.assigns.flash, :info) == "Welcome to this Logflare team!"
  end

  test "not authenticated user cannot switch teams", %{conn: conn} do
    assert conn
           |> delete(~p"/profile/switch")
           |> redirected_to(302) == ~p"/auth/login"
  end

  test "switching teams sets last team cookie with team_id for team_user", %{conn: conn} do
    user = insert(:user)
    other_user = insert(:user)
    team1 = insert(:team, user: user)
    team_user1 = insert(:team_user, team: team1, email: user.email)
    team2 = insert(:team, user: other_user)
    team_user2 = insert(:team_user, team: team2, email: user.email)

    conn =
      conn
      |> login_user(user)
      |> put_session(:team_user_id, team_user1.id)
      |> get(~p"/profile/switch", %{"user_id" => user.id, "team_user_id" => team_user2.id})

    assert redirected_to(conn, 302) =~ "/dashboard"
    cookies = Map.get(conn.resp_cookies, "_logflare_last_team", %{})
    assert cookies.value == "#{team2.id}"
    assert cookies.max_age == 2_592_000
  end

  test "switching to personal account sets last team cookie with user's team_id", %{conn: conn} do
    user = insert(:user)
    team = insert(:team, user: user)
    team_user = insert(:team_user, team: team, email: user.email)

    conn =
      conn
      |> login_user(user)
      |> put_session(:team_user_id, team_user.id)
      |> get(~p"/profile/switch", %{"user_id" => user.id})

    assert redirected_to(conn, 302) == ~p"/dashboard"
    cookies = Map.get(conn.resp_cookies, "_logflare_last_team", %{})
    assert cookies.value == "#{team.id}"
    assert cookies.max_age == 2_592_000
  end

  test "switching from personal to team account sets last team cookie", %{conn: conn} do
    user = insert(:user)
    team = insert(:team, user: user)
    team_user = insert(:team_user, team: team, email: user.email)

    conn =
      conn
      |> login_user(user)
      |> get(~p"/profile/switch", %{"user_id" => user.id, "team_user_id" => team_user.id})

    assert redirected_to(conn, 302) == ~p"/dashboard"
    cookies = Map.get(conn.resp_cookies, "_logflare_last_team", %{})
    assert cookies.value == "#{team.id}"
    assert cookies.max_age == 2_592_000
  end
end
