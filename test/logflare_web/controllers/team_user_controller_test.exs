defmodule LogflareWeb.TeamUserControllerTest do
  use LogflareWeb.ConnCase

  setup do
    insert(:plan)
    :ok
  end

  test "user can edit their profile", %{conn: conn} do
    owner = insert(:user)
    member_user = insert(:user)
    team = insert(:team, user: owner)
    team_user = insert(:team_user, team: team, email: member_user.email)

    new_name = "Avengers"
    new_email = "tony.stark@avengers.com"
    new_phone = "+1 (555) 123-4567"

    conn
    |> login_user(member_user, team_user)
    |> visit(~p"/profile/edit?t=#{team.id}")
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
    |> login_user(user, team_user)
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
      |> login_user(user, team_user)
      |> delete(~p"/profile")

    assert redirected_to(conn, 302) =~ "/login"
    assert Phoenix.Flash.get(conn.assigns.flash, :info) == "Profile deleted!"
    refute Logflare.TeamUsers.get_team_user(team_user.id)
  end

  test "owner can delete a team member", %{conn: conn} do
    owner = insert(:user)
    team = insert(:team, user: owner)
    member_user = insert(:user)
    member_team_user = insert(:team_user, team: team, email: member_user.email)

    expect(
      GoogleApi.CloudResourceManager.V1.Api.Projects,
      :cloudresourcemanager_projects_set_iam_policy,
      fn _, _project_number, [body: _body] -> {:ok, ""} end
    )

    conn =
      conn
      |> login_user(owner)
      |> delete(~p"/profile/#{member_team_user.id}")

    assert redirected_to(conn, 302) == "/account/edit"
    assert Phoenix.Flash.get(conn.assigns.flash, :info) == "Member profile deleted!"
    refute Logflare.TeamUsers.get_team_user(member_team_user.id)
  end

  test "not authenticated user cannot see delete a team member", %{conn: conn} do
    team_user = insert(:team_user)

    assert conn
           |> delete(~p"/profile/#{team_user.id}")
           |> redirected_to(302) == ~p"/auth/login"
  end

  test "owner cannot delete team member from another team", %{conn: conn} do
    owner_a = insert(:user)
    _team_a = insert(:team, user: owner_a)

    owner_b = insert(:user)
    team_b = insert(:team, user: owner_b)
    victim_team_user = insert(:team_user, team: team_b, email: "victim@example.com")

    conn =
      conn
      |> login_user(owner_a)
      |> delete(~p"/profile/#{victim_team_user.id}")

    assert redirected_to(conn, 302) == "/account/edit"

    assert Phoenix.Flash.get(conn.assigns.flash, :error) ==
             "Not authorized to delete this team member"

    assert Logflare.TeamUsers.get_team_user!(victim_team_user.id)
  end

  test "team member cannot delete another team member (not owner)", %{conn: conn} do
    owner = insert(:user)
    team = insert(:team, user: owner)
    member1_user = insert(:user)
    member1_team_user = insert(:team_user, team: team, email: member1_user.email)
    member2_user = insert(:user)
    member2_team_user = insert(:team_user, team: team, email: member2_user.email)

    conn =
      conn
      |> login_user(member1_user, member1_team_user)
      |> delete(~p"/profile/#{member2_team_user.id}")

    assert redirected_to(conn, 302) == ~p"/dashboard"

    assert Enum.any?(Phoenix.Flash.get(conn.assigns.flash, :error), fn
             text when is_binary(text) -> text =~ "You're not the account owner"
             _ -> false
           end)

    assert Logflare.TeamUsers.get_team_user!(member2_team_user.id)
  end
end
