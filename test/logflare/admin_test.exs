defmodule Logflare.AdminTest do
  use Logflare.DataCase, async: false

  alias Logflare.Admin

  setup do
    insert(:plan)
    :ok
  end

  test "admin?/1" do
    user = insert(:user, admin: true)
    home_team = insert(:team, user: user)
    other_user = insert(:user)
    team_user = insert(:team_user, email: "some@other_email.com", team: home_team)
    other_team_user = insert(:team_user)

    assert Admin.admin?(user.email)
    assert Admin.admin?(team_user.email)

    refute Admin.admin?("invalid@email.com")
    refute Admin.admin?(other_user.email)
    refute Admin.admin?(other_team_user.email)
  end
end
