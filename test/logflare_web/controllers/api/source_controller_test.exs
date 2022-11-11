defmodule LogflareWeb.Api.SourceControllerTest do
  use ExUnit.Case
  use LogflareWeb.ConnCase

  import Assertions
  import Logflare.Factory

  alias Logflare.Sources.Counters

  setup do
    insert(:plan, name: "Free")
    u1 = insert(:user)
    u2 = insert(:user)

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

  describe "show/2" do
    test "returns single sources for given user", %{
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

    test "returns not found if doesn't own the source", %{
      conn: conn,
      users: [_u1, u2],
      sources: [s1 | _]
    } do
      conn
      |> login_user(u2)
      |> get("/api/sources/#{s1.token}")
      |> response(404)
    end
  end

  describe "create/2" do
    test "creates a new source for an authenticated user", %{
      conn: conn,
      users: [u1, _u2]
    } do
      name = TestUtils.random_string()

      response =
        conn
        |> login_user(u1)
        |> post("/api/sources", %{name: name})
        |> json_response(201)

      assert response["name"] == name
    end
  end

  describe "update/2" do
    test "updates an existing source from a user", %{
      conn: conn,
      users: [u1, _u2],
      sources: [s1 | _]
    } do
      name = TestUtils.random_string()

      response =
        conn
        |> login_user(u1)
        |> patch("/api/sources/#{s1.token}", %{name: name})
        |> json_response(204)

      assert response["name"] == name
    end

    test "returns not found if doesn't own the source", %{
      conn: conn,
      users: [u1, _u2],
      sources: [_s1, _s2, s3]
    } do
      conn
      |> login_user(u1)
      |> patch("/api/sources/#{s3.token}", %{name: TestUtils.random_string()})
      |> response(404)
    end
  end

  describe "delete/2" do
    test "deletes an existing source from a user", %{
      conn: conn,
      users: [u1, _u2],
      sources: [s1 | _]
    } do
      name = TestUtils.random_string()

      conn
      |> login_user(u1)
      |> delete("/api/sources/#{s1.token}", %{name: name})
      |> response(204)

      conn
      |> login_user(u1)
      |> get("/api/sources/#{s1.token}")
      |> response(404)
    end

    test "returns not found if doesn't own the source", %{
      conn: conn,
      users: [u1, _u2],
      sources: [_s1, _s2, s3]
    } do
      conn
      |> login_user(u1)
      |> delete("/api/sources/#{s3.token}", %{name: TestUtils.random_string()})
      |> response(404)
    end
  end

  test "changeset errors handled gracefully", %{
    conn: conn,
    users: [u1, _u2],
    sources: [s1 | _]
  } do
    resp =
      conn
      |> login_user(u1)
      |> post("/api/sources")
      |> json_response(422)

    assert resp == %{"errors" => %{"name" => ["can't be blank"]}}

    resp =
      conn
      |> login_user(u1)
      |> patch("/api/sources/#{s1.token}", %{name: 123})
      |> json_response(422)

    assert resp == %{"errors" => %{"name" => ["is invalid"]}}
  end
end
