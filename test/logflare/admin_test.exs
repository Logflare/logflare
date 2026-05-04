defmodule Logflare.AdminTest do
  use Logflare.DataCase, async: true

  alias Logflare.Admin

  setup do
    insert(:plan)
    :ok
  end

  test "grant_admin/2 sets admin flag on a regular user and is idempotent" do
    granter = insert(:user, admin: true)
    user = insert(:user, admin: false)

    assert {:ok, updated} = Admin.grant_admin(granter, user)
    assert updated.admin == true

    already_admin = insert(:user, admin: true)
    assert {:ok, still_admin} = Admin.grant_admin(granter, already_admin)
    assert still_admin.admin == true
  end

  test "grant_admin/2 returns not_found when target user does not exist" do
    granter = insert(:user, admin: true)
    assert {:error, :not_found} = Admin.grant_admin(granter, nil)
  end

  test "grant_admin/2 returns unauthorized when granter is not an admin" do
    non_admin = insert(:user, admin: false)
    target = insert(:user, admin: false)

    assert {:error, :unauthorized} = Admin.grant_admin(non_admin, target)
    refute Logflare.Users.get(target.id).admin
  end

  test "grant_admin/2 does not propagate admin access to the target's team members" do
    granter = insert(:user, admin: true)
    target = insert(:user, admin: false)
    target_team = insert(:team, user: target)
    team_member = insert(:team_user, email: "member@example.com", team: target_team)

    assert {:ok, updated} = Admin.grant_admin(granter, target)
    assert updated.admin == true

    refute Admin.admin?(team_member.email)
  end

  test "admin?/1" do
    user = insert(:user, admin: true)
    home_team = insert(:team, user: user)
    other_user = insert(:user)
    team_user = insert(:team_user, email: "some@other_email.com", team: home_team)
    other_team_user = insert(:team_user)

    assert Admin.admin?(user.email)

    refute Admin.admin?("invalid@email.com")
    refute Admin.admin?(other_user.email)
    # team members of an admin user must not inherit admin privileges
    refute Admin.admin?(team_user.email)
    refute Admin.admin?(other_team_user.email)
  end
end
