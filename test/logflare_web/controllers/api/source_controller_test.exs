defmodule LogflareWeb.Api.SourceControllerTest do
  use LogflareWeb.ConnCase

  import Logflare.Factory

  alias Logflare.Sources.Counters

  setup do
    insert(:plan, name: "Free")
    user = insert(:user)
    sources = insert_list(2, :source, user_id: user.id)

    Counters.start_link()

    {:ok, user: user, sources: sources}
  end

  describe "index/2" do
    test "returns list of sources for given user", %{conn: conn, user: user, sources: sources} do
      response =
        conn
        |> login_user(user)
        |> get("/api/sources")
        |> json_response(200)

      response = response |> Enum.map(& &1["id"]) |> Enum.sort()
      expected = sources |> Enum.map(& &1.id) |> Enum.sort()

      assert response == expected
    end
  end

  describe "show/2" do
    test "returns single sources for given user", %{conn: conn, user: user, sources: [source | _]} do
      response =
        conn
        |> login_user(user)
        |> get("/api/sources/#{source.token}")
        |> json_response(200)

      assert response["id"] == source.id
    end

    test "returns not found if doesn't own the source", %{conn: conn, sources: [source | _]} do
      invalid_user = insert(:user)

      conn
      |> login_user(invalid_user)
      |> get("/api/sources/#{source.token}")
      |> response(404)
    end
  end

  describe "create/2" do
    test "creates a new source for an authenticated user", %{conn: conn, user: user} do
      name = TestUtils.random_string()

      response =
        conn
        |> login_user(user)
        |> post("/api/sources", %{name: name})
        |> json_response(201)

      assert response["name"] == name
    end
  end

  describe "update/2" do
    test "updates an existing source from a user", %{
      conn: conn,
      user: user,
      sources: [source | _]
    } do
      name = TestUtils.random_string()

      response =
        conn
        |> login_user(user)
        |> patch("/api/sources/#{source.token}", %{name: name})
        |> json_response(204)

      assert response["name"] == name
    end

    test "returns not found if doesn't own the source", %{conn: conn, sources: [source | _]} do
      invalid_user = insert(:user)

      conn
      |> login_user(invalid_user)
      |> patch("/api/sources/#{source.token}", %{name: TestUtils.random_string()})
      |> response(404)
    end
  end

  describe "delete/2" do
    test "deletes an existing source from a user", %{
      conn: conn,
      user: user,
      sources: [source | _]
    } do
      name = TestUtils.random_string()

      conn
      |> login_user(user)
      |> delete("/api/sources/#{source.token}", %{name: name})
      |> response(204)

      conn
      |> login_user(user)
      |> get("/api/sources/#{source.token}")
      |> response(404)
    end

    test "returns not found if doesn't own the source", %{
      conn: conn,
      sources: [source | _]
    } do
      invalid_user = insert(:user)

      conn
      |> login_user(invalid_user)
      |> delete("/api/sources/#{source.token}", %{name: TestUtils.random_string()})
      |> response(404)
    end
  end

  test "changeset errors handled gracefully", %{conn: conn, user: user, sources: [source | _]} do
    resp =
      conn
      |> login_user(user)
      |> post("/api/sources")
      |> json_response(422)

    assert resp == %{"errors" => %{"name" => ["can't be blank"]}}

    resp =
      conn
      |> login_user(user)
      |> patch("/api/sources/#{source.token}", %{name: 123})
      |> json_response(422)

    assert resp == %{"errors" => %{"name" => ["is invalid"]}}
  end
end
