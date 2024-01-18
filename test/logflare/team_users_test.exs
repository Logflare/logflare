defmodule Logflare.TeamUsersTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.TeamUsers

  test "list_team_users_by_and_preload/1" do
     insert_list(2, :team_user, email: "some email")
     insert(:team_user, email: "other email")
     assert [%{team: %_{}}] = TeamUsers.list_team_users_by_and_preload(email: "other email")
  end

end
