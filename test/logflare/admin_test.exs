defmodule Logflare.AdminTest do
  use Logflare.DataCase, async: false

  alias Logflare.Sources
  alias Logflare.User
  alias Logflare.Users
  alias Logflare.Admin

  setup do
    insert(:plan)
    user = insert(:user)

    {:ok, user: user}
  end

  test "is_admin?/1" do
    user = insert(:user, admin: true)
    home_team = insert(:team, user: user)
    other_user = insert(:user)
    team_user = insert(:team_user, email: "some@other_email.com", team: home_team)
    other_team_user = insert(:team_user)

    assert Admin.is_admin?(user.email)
    assert Admin.is_admin?(team_user.email)

    refute Admin.is_admin?("invalid@email.com")
    refute Admin.is_admin?(other_user.email)
    refute Admin.is_admin?(other_team_user.email)
  end
end
