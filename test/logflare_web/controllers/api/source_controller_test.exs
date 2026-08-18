defmodule LogflareWeb.Api.SourceControllerTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  alias Logflare.Backends
  alias Logflare.Backends.SourceSup
  alias Logflare.Sources
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

  describe "index/2 with redacted backend configs" do
    @backend_configs_by_type [
      {:webhook, %{url: "http://example.com", headers: %{"Authorization" => "leaked-secret"}}},
      {:elastic, %{url: "https://example.com", username: "someuser", password: "leaked-secret"}},
      {:datadog, %{api_key: "leaked-secret", region: "US1"}},
      {:sentry, %{dsn: "https://user:leaked-secret@sentry.io/123"}},
      {:postgres, %{url: "postgresql://user:leaked-secret@localhost:5432/db"}},
      {:loki, %{url: "https://example.com", username: "someuser", password: "leaked-secret"}},
      {:clickhouse,
       %{
         url: "http://localhost:8123",
         username: "someuser",
         password: "leaked-secret",
         database: "default",
         port: 8123
       }},
      {:incidentio, %{api_token: "leaked-secret", alert_source_config_id: "abc"}},
      {:s3,
       %{
         endpoint: "https://s3.amazonaws.com",
         s3_bucket: "my-bucket",
         storage_region: "us-east-1",
         access_key_id: "AKIA_ID",
         secret_access_key: "leaked-secret"
       }},
      {:axiom, %{domain: "api.axiom.co", api_token: "leaked-secret", dataset_name: "ds"}},
      {:otlp, %{endpoint: "http://example.com", headers: %{"Authorization" => "leaked-secret"}}},
      {:last9, %{region: "us-east-1", username: "someuser", password: "leaked-secret"}},
      {:syslog, %{host: "example.com", port: 514, cipher_key: "leaked-secret"}}
    ]

    for {type, config} <- @backend_configs_by_type do
      test "GET /api/sources redacts secrets for #{type} backend attached as default ingest",
           %{conn: conn, user: user, sources: [source | _]} do
        insert(:backend,
          type: unquote(type),
          config: unquote(Macro.escape(config)),
          user: user,
          sources: [source],
          default_ingest?: true
        )

        response =
          conn
          |> add_access_token(user, "private")
          |> get("/api/sources")
          |> json_response(200)

        assert %{"backends" => [backend]} = Enum.find(response, &(&1["id"] == source.id))

        refute Jason.encode!(backend["config"]) =~ "leaked-secret"
      end
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

    test "attacker cannot attach their backend to another user's source through the api",
         %{conn: conn} do
      attacker = insert(:user)
      victim = insert(:user)

      attacker_backend = insert(:backend, user: attacker)
      victim_source = insert(:source, user: victim)

      conn
      |> add_access_token(attacker, "private")
      |> post(~p"/api/sources/#{victim_source.token}/backends/#{attacker_backend.token}")

      source =
        victim_source.id
        |> Sources.get()
        |> Sources.preload_backends()

      refute Enum.any?(source.backends, &(&1.id == attacker_backend.id))
    end

    test "attacker cannot attach another user's backend to their own source",
         %{conn: conn} do
      attacker = insert(:user)
      victim = insert(:user)

      victim_backend = insert(:backend, user: victim)
      attacker_source = insert(:source, user: attacker)

      conn
      |> add_access_token(attacker, "private")
      |> post(~p"/api/sources/#{attacker_source.token}/backends/#{victim_backend.token}")

      source =
        attacker_source.id
        |> Sources.get()
        |> Sources.preload_backends()

      refute Enum.any?(source.backends, &(&1.id == victim_backend.id))
      assert source.backends == []
    end

    test "attacker cannot remove a backend from another user's source through the api",
         %{conn: conn} do
      attacker = insert(:user)
      victim = insert(:user)

      victim_source = insert(:source, user: victim)
      victim_backend = insert(:backend, user: victim, sources: [victim_source])

      conn
      |> add_access_token(attacker, "private")
      |> delete(~p"/api/sources/#{victim_source.token}/backends/#{victim_backend.token}")

      source =
        victim_source.id
        |> Sources.get()
        |> Sources.preload_backends()

      assert Enum.any?(source.backends, &(&1.id == victim_backend.id))
      assert [_] = source.backends
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
