defmodule LogflareWeb.Api.SourceControllerTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  alias Logflare.Backends
  alias Logflare.Source.RecentLogsServer

  setup do
    insert(:plan, name: "Free")
    user = insert(:user)
    sources = insert_list(2, :source, user_id: user.id)

    {:ok, user: user, sources: sources}
  end

  describe "index/2" do
    test "returns list of sources for given user", %{conn: conn, user: user, sources: sources} do
      response =
        conn
        |> add_access_token(user, "private")
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
        |> add_access_token(user, "private")
        |> get("/api/sources/#{source.token}")
        |> json_response(200)

      assert response["id"] == source.id
    end

    test "backend postgres secrets are redacted", %{conn: conn, user: user, sources: [source | _]} do
      insert(:backend,
        sources: [source],
        user: user,
        type: :postgres,
        config: %{url: "postgresql://user:secret@localhost"}
      )

      assert %{"backends" => [backend]} =
               conn
               |> add_access_token(user, "private")
               |> get("/api/sources/#{source.token}")
               |> json_response(200)

      config = backend["config"]
      assert config["url"] =~ "postgresql://user:REDACTED@localhost"
    end

    test "returns not found if doesn't own the source", %{conn: conn, sources: [source | _]} do
      invalid_user = insert(:user)

      conn
      |> add_access_token(invalid_user, "private")
      |> get("/api/sources/#{source.token}")
      |> response(404)
    end
  end

  describe "create/2" do
    test "creates a new source for an authenticated user", %{conn: conn, user: user} do
      name = TestUtils.random_string()

      response =
        conn
        |> add_access_token(user, "private")
        |> post("/api/sources", %{name: name})
        |> json_response(201)

      assert response["name"] == name
    end

    test "returns 422 on missing arguments", %{conn: conn, user: user} do
      resp =
        conn
        |> add_access_token(user, "private")
        |> post("/api/sources")
        |> json_response(422)

      assert resp == %{"errors" => %{"name" => ["can't be blank"]}}
    end

    test "returns 422 on bad arguments", %{conn: conn, user: user} do
      resp =
        conn
        |> add_access_token(user, "private")
        |> post("/api/sources", %{name: 123})
        |> json_response(422)

      assert resp == %{"errors" => %{"name" => ["is invalid"]}}
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
        |> add_access_token(user, "private")
        |> patch("/api/sources/#{source.token}", %{name: name})
        |> json_response(204)

      assert response["name"] == name
    end

    test "returns not found if doesn't own the source", %{conn: conn, sources: [source | _]} do
      invalid_user = insert(:user)

      conn
      |> add_access_token(invalid_user, "private")
      |> patch("/api/sources/#{source.token}", %{name: TestUtils.random_string()})
      |> response(404)
    end

    test "returns 422 on bar arguments", %{conn: conn, user: user, sources: [source | _]} do
      resp =
        conn
        |> add_access_token(user, "private")
        |> patch("/api/sources/#{source.token}", %{name: 123})
        |> json_response(422)

      assert resp == %{"errors" => %{"name" => ["is invalid"]}}
    end
  end

  describe "add_backend/2" do
    test "attaches a backend", %{conn: conn, user: user, sources: [source | _]} do
      backend = insert(:backend, user: user)

      conn =
        conn
        |> add_access_token(user, "private")
        |> post("/api/sources/#{source.token}/backends/#{backend.token}")

      # returns the source
      assert %{"token" => _, "backends" => [_]} = json_response(conn, 201)
    end

    test "removes a backend", %{conn: conn, user: user, sources: [source | _]} do
      backend = insert(:backend, user: user, sources: [source])

      conn =
        conn
        |> add_access_token(user, "private")
        |> delete("/api/sources/#{source.token}/backends/#{backend.token}")

      # returns the source
      assert %{"token" => _, "backends" => []} = json_response(conn, 200)
    end
  end

  describe "recent/2" do
    test "able to view recent logs", %{conn: conn, user: user, sources: [source | _]} do
      start_link_supervised!({RecentLogsServer, source: source})
      le = build(:log_event, source: source, message: "something")
      Backends.push_recent_events(source, [le])

      conn =
        conn
        |> add_access_token(user, "private")
        |> get("/api/sources/#{source.token}/recent")

      assert [%{"event_message" => "something", "timestamp" => _}] = json_response(conn, 200)
    end
  end

  describe "delete/2" do
    test "deletes an existing source from a user", %{
      conn: conn,
      user: user,
      sources: [source | _]
    } do
      name = TestUtils.random_string()

      assert conn
             |> add_access_token(user, "private")
             |> delete("/api/sources/#{source.token}", %{name: name})
             |> response(204)

      assert conn
             |> add_access_token(user, "private")
             |> get("/api/sources/#{source.token}")
             |> response(404)
    end

    test "returns not found if doesn't own the source", %{
      conn: conn,
      sources: [source | _]
    } do
      invalid_user = insert(:user)

      assert conn
             |> add_access_token(invalid_user, "private")
             |> delete("/api/sources/#{source.token}", %{name: TestUtils.random_string()})
             |> response(404)
    end
  end
end
