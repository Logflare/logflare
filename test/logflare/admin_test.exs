defmodule Logflare.AdminTest do
  use Logflare.DataCase, async: true

  alias Logflare.Admin

  setup do
    insert(:plan)
    :ok
  end

  test "grant_admin/1 sets admin flag on a regular user" do
    user = insert(:user, admin: false)
    assert {:ok, updated} = Admin.grant_admin(user)
    assert updated.admin == true
  end

  test "grant_admin/1 is idempotent for an existing admin" do
    admin = insert(:user, admin: true)
    assert {:ok, updated} = Admin.grant_admin(admin)
    assert updated.admin == true
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
