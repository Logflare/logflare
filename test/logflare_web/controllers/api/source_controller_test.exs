defmodule LogflareWeb.Api.SourceControllerTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  alias Logflare.Backends
  alias Logflare.Backends.SourceSup
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.TestUtils

  setup do
    start_supervised!(AllLogsLogged)
    insert(:plan, name: "Free")
    user = insert(:user)
    sources = insert_list(2, :source, user_id: user.id, description: TestUtils.random_string())

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
      assert response["description"] == source.description
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
      description = "My new source"

      response =
        conn
        |> add_access_token(user, "private")
        |> post("/api/sources", %{name: name, description: description})
        |> json_response(201)

      assert response["name"] == name
      assert response["description"] == description
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

    test "creates a source with `default_ingest_backend_enabled?` true", %{conn: conn, user: user} do
      name = TestUtils.random_string()

      response =
        conn
        |> add_access_token(user, "private")
        |> post("/api/sources", %{name: name, default_ingest_backend_enabled?: true})
        |> json_response(201)

      assert response["name"] == name
      assert response["default_ingest_backend_enabled?"] == true
    end

    test "creates a source without `default_ingest_backend_enabled?` field (defaults to false)",
         %{
           conn: conn,
           user: user
         } do
      name = TestUtils.random_string()

      response =
        conn
        |> add_access_token(user, "private")
        |> post("/api/sources", %{name: name})
        |> json_response(201)

      assert response["name"] == name
      assert response["default_ingest_backend_enabled?"] == false
    end
  end

  describe "update/2" do
    test "PUT updates an existing source from a user", %{
      conn: conn,
      user: user,
      sources: [source | _]
    } do
      name = TestUtils.random_string()
      description = TestUtils.random_string()

      response =
        conn
        |> add_access_token(user, "private")
        |> put("/api/sources/#{source.token}", %{name: name, description: description})
        |> json_response(200)

      assert response["id"] == source.id
      assert response["name"] == name
      assert response["description"] == description
    end

    test "PATCH updates an existing source from a user", %{
      conn: conn,
      user: user,
      sources: [source | _]
    } do
      name = TestUtils.random_string()

      response =
        conn
        |> add_access_token(user, "private")
        |> patch("/api/sources/#{source.token}", %{name: name})
        |> response(204)

      assert response == ""
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

    test "PATCH updates `default_ingest_backend_enabled?` field", %{
      conn: conn,
      user: user,
      sources: [source | _]
    } do
      conn
      |> add_access_token(user, "private")
      |> patch("/api/sources/#{source.token}", %{default_ingest_backend_enabled?: true})
      |> response(204)

      response =
        conn
        |> add_access_token(user, "private")
        |> get("/api/sources/#{source.token}")
        |> json_response(200)

      assert response["default_ingest_backend_enabled?"] == true
    end
  end

  describe "transformations" do
  end

  describe "retention_days" do
    setup do
      Logflare.Google.BigQuery
      |> expect(:patch_table_ttl, fn _source_id, _table_ttl, _dataset_id, _project_id ->
        {:ok, %Tesla.Env{}}
      end)

      :ok
    end

    test "PUT updates retention_days", %{
      conn: conn,
      user: user,
      sources: [source | _]
    } do
      assert %{"retention_days" => 3} =
               conn
               |> add_access_token(user, "private")
               |> get("/api/sources/#{source.token}")
               |> json_response(200)

      assert %{"retention_days" => 1} =
               conn
               |> add_access_token(user, "private")
               |> put("/api/sources/#{source.token}", %{name: "some name", retention_days: 1})
               |> json_response(200)
    end

    test "PATCH updates retention_days", %{
      conn: conn,
      user: user,
      sources: [source | _]
    } do
      conn
      |> add_access_token(user, "private")
      |> patch("/api/sources/#{source.token}", %{retention_days: 1})
      |> response(204)
    end
  end

  describe "show_schema/2" do
    test "GET schema with dot syntax", %{conn: conn, user: user, sources: [source | _]} do
      insert(:source_schema,
        source: source,
        bigquery_schema:
          TestUtils.build_bq_schema(%{
            "test" => %{"nested" => 123}
          })
      )

      conn =
        conn
        |> add_access_token(user, "private")
        |> get("/api/sources/#{source.token}/schema?variant=dot")

      # returns the source
      assert %{
               "id" => "string",
               "event_message" => "string",
               "timestamp" => "datetime",
               "test.nested" => "integer"
             } = json_response(conn, 200)
    end

    test "GET schema with json schema", %{conn: conn, user: user, sources: [source | _]} do
      insert(:source_schema,
        source: source,
        bigquery_schema:
          TestUtils.build_bq_schema(%{
            "test" => %{"nested" => 123, "listical" => ["testing", "123"]}
          })
      )

      %{name: source_name} = source

      conn =
        conn
        |> add_access_token(user, "private")
        |> get("/api/sources/#{source.token}/schema")

      # returns the source
      assert %{
               "$schema" => _,
               "$id" => _,
               "title" => ^source_name,
               "type" => "object",
               "properties" => %{
                 "id" => %{"type" => "string"},
                 "event_message" => %{"type" => "string"},
                 "timestamp" => %{"type" => "number"}
               }
             } = json_response(conn, 200)
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
      start_supervised!({SourceSup, source})

      le = build(:log_event, source: source, message: "something")
      Backends.ingest_logs([le], source)

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
