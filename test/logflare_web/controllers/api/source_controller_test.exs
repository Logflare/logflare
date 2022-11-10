defmodule LogflareWeb.Api.SourceControllerTest do
  use ExUnit.Case
  use LogflareWeb.ConnCase

  import Logflare.Factory
  import Assertions

  alias Logflare.Sources
  alias Logflare.Sources.Counters
  alias Logflare.Teams

  setup do
    insert(:plan, name: "Free")
    u1 = insert(:user)
    u2 = insert(:user)
    Teams.create_team(u1, %{name: "u1 team"})
    Teams.create_team(u2, %{name: "u2 team"})

    s1 = insert(:source, public_token: Faker.String.base64(16), user_id: u1.id)
    s2 = insert(:source, user_id: u1.id)
    s3 = insert(:source, user_id: u2.id)

    Counters.start_link()

    {:ok, users: [u1, u2], sources: [s1, s2, s3]}
  end

  describe "index/2" do
    test "returns list of sources for given user", %{
      conn: conn,
      users: [u1, _u2],
      sources: [s1, s2 | _]
    } do
      response =
        conn
        |> login_user(u1)
        |> get("/api/sources")
        |> json_response(200)

      response = Enum.map(response, & &1["id"])
      expected = Enum.map([s1, s2], & &1.id)

      assert_lists_equal(response, expected)
    end
  end

  describe "create/2" do
    test "returns list of sources for given user", %{
      conn: conn,
      users: [u1, _u2]
    } do
      name = TestUtils.random_string()

      conn
      |> login_user(u1)
      |> post("/api/sources", %{name: name})
      |> response(204)

      assert Enum.find(Sources.get_sources_by_user(u1), &(&1.name == name))
    end

    test "changeset errors handled gracefully", %{
      conn: conn,
      users: [u1, _u2]
    } do
      resp =
        conn
        |> login_user(u1)
        |> post("/api/sources")
        |> json_response(422)

      assert resp == %{"errors" => %{"name" => ["can't be blank"]}}
    end
  end
end
