defmodule LogflareWeb.CommandPaletteControllerTest do
  use LogflareWeb.ConnCase

  setup do
    insert(:plan)
    :ok
  end

  describe "GET /command-palette/sources" do
    test "aggregates sources across every team the user can access", %{conn: conn} do
      user = insert(:user)
      _home_team = insert(:team, user: user, name: "Home")

      other_owner = insert(:user)
      other_team = insert(:team, user: other_owner, name: "Other")
      insert(:team_user, team: other_team, email: user.email)

      _stranger_owner = insert(:user)
      stranger_team = insert(:team, user: insert(:user), name: "Stranger")

      home_source = insert(:source, user: user, name: "home-source")
      other_source = insert(:source, user: other_owner, name: "other-source")
      _stranger_source = insert(:source, user: stranger_team.user, name: "stranger-source")
      _system = insert(:source, user: user, name: "sys", system_source: true)

      conn =
        conn
        |> login_user(user)
        |> get(~p"/command-palette/sources")

      %{"sources" => sources} = json_response(conn, 200)

      names = sources |> Enum.map(& &1["name"]) |> Enum.sort()
      assert names == Enum.sort([home_source.name, other_source.name])

      by_name = Map.new(sources, &{&1["name"], &1})
      assert by_name[home_source.name]["team"]["name"] == "Home"
      assert by_name[other_source.name]["team"]["name"] == "Other"
      assert is_integer(by_name[home_source.name]["team"]["id"])
    end

    test "returns an empty list when the user has no sources", %{conn: conn} do
      user = insert(:user)
      insert(:team, user: user)

      conn =
        conn
        |> login_user(user)
        |> get(~p"/command-palette/sources")

      assert %{"sources" => []} = json_response(conn, 200)
    end

    test "redirects to login when unauthenticated", %{conn: conn} do
      conn = get(conn, ~p"/command-palette/sources")
      assert redirected_to(conn) =~ "/auth/login"
    end
  end
end
