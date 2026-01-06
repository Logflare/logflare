defmodule LogflareWeb.TeamUserControllerTest do
  use LogflareWeb.ConnCase

  alias Logflare.TeamUsers

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
    |> assert_has("#flash-info", text: "Profile updated!")
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
    owner = insert(:user)
    team = insert(:team, user: owner)
    team_user = insert(:team_user, team: team)

    expect(
      GoogleApi.CloudResourceManager.V1.Api.Projects,
      :cloudresourcemanager_projects_set_iam_policy,
      fn _, _project_number, [body: _body] -> {:ok, ""} end
    )

    conn
    |> login_user(owner, team_user)
    |> visit(~p"/profile/edit?t=#{team.id}")
    |> assert_has("h5", text: "Profile Preferences", exact: true)
    |> click_link("Leave now")
    |> assert_path(~p"/auth/login", query_params: %{team_user_deleted: "true"})

    refute TeamUsers.get_team_user(team_user.id)
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

    assert redirected_to(conn, 302) == "/account/edit?t=#{team.id}#team-members"
    assert Phoenix.Flash.get(conn.assigns.flash, :info) == "Member profile deleted!"
    refute TeamUsers.get_team_user(member_team_user.id)
  end

  test "not authenticated user cannot see delete a team member", %{conn: conn} do
    team_user = insert(:team_user)

    assert conn
           |> delete(~p"/profile/#{team_user.id}")
           |> redirected_to(302) == ~p"/auth/login"
  end

  test "owner cannot delete team member from another team", %{conn: conn} do
    owner_a = insert(:user)
    team_a = insert(:team, user: owner_a)

    owner_b = insert(:user)
    team_b = insert(:team, user: owner_b)
    victim_team_user = insert(:team_user, team: team_b, email: "victim@example.com")

    conn =
      conn
      |> login_user(owner_a)
      |> delete(~p"/profile/#{victim_team_user.id}")

    assert redirected_to(conn, 302) == "/account/edit?t=#{team_a.id}#team-members"

    assert Phoenix.Flash.get(conn.assigns.flash, :error) ==
             "Not authorized to delete this team member"

    assert TeamUsers.get_team_user!(victim_team_user.id)
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

  test "update re-renders edit form with submitted values when context returns error",
       %{conn: conn} do
    owner = insert(:user)
    team = insert(:team, user: owner)
    team_user = insert(:team_user, team: team)

    expect(TeamUsers, :update_team_user, fn tu, params ->
      changeset =
        tu
        |> TeamUsers.TeamUser.changeset(params)
        |> Ecto.Changeset.add_error(:email_preferred, "is invalid")

      {:error, changeset}
    end)

    conn
    |> login_user(owner, team_user)
    |> visit(~p"/profile/edit?t=#{team.id}")
    |> fill_in("Name", with: "Bruce Banner")
    |> fill_in("Preferred email", with: "hulk@avengers.com")
    |> click_button("Update preferences")
    |> assert_has("#flash-error", text: "Something went wrong!")
    |> assert_has("input#team_user_name", value: "Bruce Banner")
    |> assert_has("input#team_user_email_preferred", value: "hulk@avengers.com")
  end

  test "delete_self re-renders edit form when context returns error", %{conn: conn} do
    owner = insert(:user)
    team = insert(:team, user: owner)
    team_user = insert(:team_user, team: team)

    expect(TeamUsers, :delete_team_user, fn tu ->
      changeset =
        tu
        |> Ecto.Changeset.change()
        |> Ecto.Changeset.add_error(:base, "cannot delete")

      {:error, changeset}
    end)

    conn
    |> login_user(owner, team_user)
    |> visit(~p"/profile/edit?t=#{team.id}")
    |> click_link("Leave now")
    |> assert_has("#flash-error", text: "Something went wrong!")
    |> assert_has("h5", text: "Profile Preferences", exact: true)

    assert TeamUsers.get_team_user(team_user.id)
  end

  test "edit page shows GitHub-specific section for github provider", %{conn: conn} do
    owner = insert(:user)
    team = insert(:team, user: owner)
    team_user = insert(:team_user, team: team, provider: "github")

    conn
    |> login_user(owner, team_user)
    |> visit(~p"/profile/edit?t=#{team.id}")
    |> assert_has("h5", text: "Signed In with Github")
    |> assert_has("a[href='https://github.com/settings/applications']", text: "Visit Github")
    |> refute_has("a", text: "Visit Google")
  end

  test "edit page shows Google-specific section for google provider", %{conn: conn} do
    owner = insert(:user)
    team = insert(:team, user: owner)
    team_user = insert(:team_user, team: team, provider: "google")

    conn
    |> login_user(owner, team_user)
    |> visit(~p"/profile/edit?t=#{team.id}")
    |> assert_has("h5", text: "Signed In with Google")
    |> assert_has("a", text: "Visit Google")
    |> refute_has("a", text: "Visit Github")
  end

  test "user can toggle email_me_product preference off", %{conn: conn} do
    owner = insert(:user)
    team = insert(:team, user: owner)
    team_user = insert(:team_user, team: team, email_me_product: true)

    conn
    |> login_user(owner, team_user)
    |> visit(~p"/profile/edit?t=#{team.id}")
    |> uncheck("Email me product updates")
    |> click_button("Update preferences")
    |> assert_has("#flash-info", text: "Profile updated!")

    assert Logflare.Repo.reload!(team_user).email_me_product == false
  end

  describe "team member management as team admin" do
    test "can grant and revoke admin role", %{conn: conn} do
      owner = insert(:user)
      team = insert(:team, user: owner)
      admin_user = insert(:user)

      admin_team_user =
        insert(:team_user, team: team, email: admin_user.email, team_role: %{role: :admin})

      admin_team_user = Logflare.TeamUsers.preload_defaults(admin_team_user)
      regular_team_user = insert(:team_user, team: team)

      assert regular_team_user.team_role.role == :user

      session =
        conn
        |> login_user(admin_team_user)
        |> visit(~p"/profile/edit?t=#{team.id}")

      session
      |> within("#team-admin-id-#{regular_team_user.id}", fn session ->
        session
        |> check("admin")
        |> submit()
        |> assert_path("/profile/edit", query_params: %{t: team.id})
        |> unwrap(fn conn ->
          assert Phoenix.Flash.get(conn.assigns.flash, :info) ==
                   "#{regular_team_user.email} role updated."

          conn
        end)

        regular_team_user = Logflare.TeamUsers.get_team_user_and_preload(regular_team_user.id)
        assert regular_team_user.team_role.role == :admin

        session
      end)

      session
      |> within("#team-admin-id-#{admin_team_user.id}", fn session ->
        session
        |> uncheck("admin")
        |> submit()
        |> assert_path("/profile/edit", query_params: %{t: team.id})
        |> unwrap(fn conn ->
          assert Phoenix.Flash.get(conn.assigns.flash, :info) ==
                   "#{admin_team_user.email} role updated."

          conn
        end)

        admin_team_user = Logflare.TeamUsers.get_team_user_and_preload(admin_team_user.id)
        refute admin_team_user.team_role.role == :admin

        session
      end)
    end

    test "can delete another team member", %{conn: conn} do
      owner = insert(:user)
      team = insert(:team, user: owner)
      admin_user = insert(:user)

      admin_team_user =
        insert(:team_user, team: team, email: admin_user.email, team_role: %{role: :admin})

      admin_team_user = Logflare.TeamUsers.preload_defaults(admin_team_user)
      regular_team_user = insert(:team_user, team: team)

      expect(
        GoogleApi.CloudResourceManager.V1.Api.Projects,
        :cloudresourcemanager_projects_set_iam_policy,
        fn _, _project_number, [body: _body] -> {:ok, ""} end
      )

      session =
        conn
        |> login_user(admin_user, admin_team_user)
        |> visit(~p"/profile/edit?t=#{team.id}")
        |> click_link("a[href='#{~p"/profile/#{regular_team_user.id}?t=#{team.id}"}']", "Delete")
        |> assert_path("/profile/edit", query_params: %{t: team.id})

      session
      |> unwrap(fn conn ->
        assert Phoenix.Flash.get(conn.assigns.flash, :info) ==
                 "Member profile deleted!"

        conn
      end)

      refute Logflare.TeamUsers.get_team_user(regular_team_user.id)
    end
  end

  describe "team member management without admin role" do
    test "cannot update another team member's role", %{conn: conn} do
      owner = insert(:user)
      team = insert(:team, user: owner)
      member1_user = insert(:user)
      member1_team_user = insert(:team_user, team: team, email: member1_user.email)
      member2_team_user = insert(:team_user, team: team)

      conn =
        conn
        |> login_user(member1_user, member1_team_user)

      conn
      |> visit(~p"/profile/edit?t=#{team.id}")
      |> refute_has("form#team-admin-id-#{member2_team_user.id}")

      conn =
        conn
        |> patch(~p"/profile/#{member2_team_user.id}/role", %{
          "team_role" => %{"is_admin" => "true"}
        })

      assert redirected_to(conn, 302) == ~p"/dashboard"

      assert Enum.any?(Phoenix.Flash.get(conn.assigns.flash, :error), fn
               text when is_binary(text) -> text =~ "You're not the account owner or an admin"
               _ -> false
             end)

      member2_team_user = Logflare.TeamUsers.get_team_user_and_preload(member2_team_user.id)
      assert member2_team_user.team_role.role == :user
    end
  end
end
