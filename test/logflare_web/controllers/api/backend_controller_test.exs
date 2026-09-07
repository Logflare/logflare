defmodule LogflareWeb.Api.BackendControllerTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  setup do
    insert(:plan, name: "Free")
    user = insert(:user)

    {:ok, user: user}
  end

  describe "index/2" do
    test "returns list of backends for given user", %{conn: conn, user: user} do
      insert(:backend)
      %_{id: id} = insert(:backend, user: user)

      assert [%{"id" => ^id, "inserted_at" => _, "updated_at" => _}] =
               conn
               |> add_access_token(user, "private")
               |> get(~p"/api/backends")
               |> json_response(200)
    end

    test "includes team-owned backends", %{conn: conn} do
      member = insert(:user)
      team_user = insert(:team_user, email: member.email)
      backend = insert(:backend, user: team_user.team.user)
      unrelated_backend = insert(:backend)

      response =
        conn
        |> add_access_token(member, "private")
        |> get(~p"/api/backends")
        |> json_response(200)

      backend_ids = MapSet.new(Enum.map(response, & &1["id"]))
      assert backend.id in backend_ids
      refute unrelated_backend.id in backend_ids

      assert %{"id" => backend_id} =
               conn
               |> add_access_token(member, "private")
               |> get(~p"/api/backends/#{backend.token}")
               |> json_response(200)

      assert backend_id == backend.id
    end

    test "can filter on metadata column", %{conn: conn, user: user} do
      insert(:backend, user: user)
      backend = insert(:backend, user: user, metadata: %{my: "field", data: true})

      assert [result] =
               conn
               |> add_access_token(user, "private")
               |> get(~p"/api/backends?#{%{metadata: %{my: "field", data: true}}}")
               |> json_response(200)

      assert result["id"] == backend.id
    end

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
      test "GET /api/backends redacts secrets for #{type} backend attached as default ingest",
           %{conn: conn, user: user} do
        source = insert(:source, user: user)

        %_{id: id} =
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
          |> get(~p"/api/backends")
          |> json_response(200)

        assert %{"id" => ^id} = backend = Enum.find(response, &(&1["id"] == id))

        refute Jason.encode!(backend["config"]) =~ "leaked-secret"
      end
    end

    test "redacts persisted webhook header and URL credentials in the raw response", %{
      conn: conn,
      user: user
    } do
      %_{id: id} =
        insert(:backend,
          type: :webhook,
          user: user,
          config: %{
            url: "https://user:url-leaked-secret@example.com/hooks",
            headers: %{
              "Content-Type" => "application/json",
              "X-Webhook-Secret" => "header-leaked-secret"
            }
          }
        )

      conn =
        conn
        |> add_access_token(user, "private")
        |> get(~p"/api/backends")

      refute conn.resp_body =~ "url-leaked-secret"
      refute conn.resp_body =~ "header-leaked-secret"

      response = json_response(conn, 200)
      assert %{"id" => ^id} = backend = Enum.find(response, &(&1["id"] == id))

      assert %{
               "url" => "https://REDACTED@example.com/hooks",
               "headers" => %{
                 "content-type" => "application/json",
                 "x-webhook-secret" => "REDACTED"
               }
             } = backend["config"]
    end
  end

  describe "show/2" do
    test "returns a backend without webhook credentials in the raw response", %{
      conn: conn,
      user: user
    } do
      backend =
        insert(:backend,
          type: :webhook,
          user: user,
          config: %{
            url: "https://user:url-leaked-secret@example.com/hooks",
            headers: %{"X-Auth-Token" => "header-leaked-secret"}
          }
        )

      conn =
        conn
        |> add_access_token(user, "private")
        |> get("/api/backends/#{backend.token}")

      refute conn.resp_body =~ "url-leaked-secret"
      refute conn.resp_body =~ "header-leaked-secret"

      assert %{
               "id" => backend_id,
               "config" => %{
                 "url" => "https://REDACTED@example.com/hooks",
                 "headers" => %{"x-auth-token" => "REDACTED"}
               }
             } = json_response(conn, 200)

      assert backend_id == backend.id
    end

    test "serializes a persisted webhook with nil headers", %{conn: conn, user: user} do
      backend =
        insert(:backend,
          type: :webhook,
          user: user,
          config: %{url: "https://example.com/hooks", headers: nil}
        )

      assert %{"config" => %{"headers" => %{}}} =
               conn
               |> add_access_token(user, "private")
               |> get("/api/backends/#{backend.token}")
               |> json_response(200)
    end

    test "returns not found if doesn't own the source", %{conn: conn} do
      backend = insert(:backend)
      invalid_user = insert(:user)

      conn
      |> add_access_token(invalid_user, "private")
      |> get("/api/backends/#{backend.token}")
      |> response(404)
    end
  end

  describe "create/2" do
    test "creates a webhook backend for an authenticated user", %{conn: conn, user: user} do
      name = TestUtils.random_string()

      response =
        conn
        |> add_access_token(user, "private")
        |> post("/api/backends", %{
          name: name,
          type: "webhook",
          config: %{url: "http://example.com"}
        })
        |> json_response(201)

      assert response["name"] == name
      assert response["config"]["url"] =~ "example.com"
      assert response["inserted_at"]
      assert response["updated_at"]
    end

    test "creates a postgres backend for an authenticated user", %{conn: conn, user: user} do
      name = TestUtils.random_string()

      conn =
        conn
        |> add_access_token(user, "private")
        |> post("/api/backends", %{
          name: name,
          type: "postgres",
          config: %{url: "postgresql://test:my-password@localhost:5432", schema: "_my_schema"},
          description: "some description",
          metadata: %{
            some: "data"
          }
        })

      assert %{
               "id" => _,
               "token" => _,
               "name" => ^name,
               "description" => "some description",
               "config" => %{
                 "url" => "postgresql://test:REDACTED@" <> _,
                 "schema" => "_my_schema"
               },
               "metadata" => %{
                 "some" => "data"
               },
               "inserted_at" => _,
               "updated_at" => _
             } = json_response(conn, 201)
    end

    test "creates a clickhouse backend for an authenticated user", %{conn: conn, user: user} do
      name = TestUtils.random_string()

      conn =
        conn
        |> add_access_token(user, "private")
        |> post("/api/backends", %{
          name: name,
          type: "clickhouse",
          config: %{
            url: "http://localhost:8123",
            username: "test_user",
            password: "test_password",
            database: "default",
            port: 8123,
            read_pool_size: 10
          },
          description: "some description",
          metadata: %{
            some: "data"
          }
        })

      assert %{
               "id" => _,
               "token" => _,
               "name" => ^name,
               "description" => "some description",
               "config" => %{
                 "url" => "http://localhost:8123",
                 "username" => "test_user",
                 "password" => "REDACTED",
                 "database" => "default",
                 "port" => 8123,
                 "read_pool_size" => 10
               },
               "metadata" => %{
                 "some" => "data"
               }
             } = json_response(conn, 201)
    end

    test "creates a datadog backend for an authenticated user", %{conn: conn, user: user} do
      name = TestUtils.random_string()

      conn =
        conn
        |> add_access_token(user, "private")
        |> post("/api/backends", %{
          name: name,
          type: "datadog",
          config: %{api_key: "1234", region: "US1"},
          metadata: %{
            some: "data"
          }
        })

      assert %{
               "id" => _,
               "token" => _,
               "name" => ^name,
               "config" => %{
                 "api_key" => "REDACTED",
                 "region" => "US1"
               },
               "metadata" => %{
                 "some" => "data"
               }
             } = json_response(conn, 201)
    end

    test "creates a elastic backend for an authenticated user", %{conn: conn, user: user} do
      name = TestUtils.random_string()

      conn =
        conn
        |> add_access_token(user, "private")
        |> post("/api/backends", %{
          name: name,
          type: "elastic",
          config: %{url: "https://example.com", username: "someuser", password: "12345"},
          metadata: %{
            some: "data"
          }
        })

      assert %{
               "id" => _,
               "token" => _,
               "name" => ^name,
               "config" => %{
                 "url" => "https://" <> _,
                 "password" => "REDACTED",
                 "username" => "someuser"
               },
               "metadata" => %{
                 "some" => "data"
               }
             } = json_response(conn, 201)
    end

    test "creates a loki backend for an authenticated user", %{conn: conn, user: user} do
      name = TestUtils.random_string()

      conn =
        conn
        |> add_access_token(user, "private")
        |> post("/api/backends", %{
          name: name,
          type: "loki",
          config: %{url: "https://example.com", username: "someuser", password: "12345"},
          metadata: %{
            some: "data"
          }
        })

      assert %{
               "id" => _,
               "token" => _,
               "name" => ^name,
               "config" => %{
                 "url" => "https://" <> _,
                 "password" => "REDACTED",
                 "username" => "someuser"
               },
               "metadata" => %{
                 "some" => "data"
               }
             } = json_response(conn, 201)
    end

    test "returns 422 on missing arguments", %{conn: conn, user: user} do
      resp =
        conn
        |> add_access_token(user, "private")
        |> post("/api/backends")
        |> json_response(422)

      assert %{"errors" => %{"name" => ["can't be blank"], "config" => _, "type" => _}} = resp
    end

    test "returns 422 on bad arguments", %{conn: conn, user: user} do
      resp =
        conn
        |> add_access_token(user, "private")
        |> post("/api/backends", %{name: 123})
        |> json_response(422)

      assert %{"errors" => %{"name" => ["is invalid"]}} = resp
    end

    test "creates a clickhouse backend with `default_ingest?` true", %{conn: conn, user: user} do
      name = TestUtils.random_string()

      response =
        conn
        |> add_access_token(user, "private")
        |> post("/api/backends", %{
          name: name,
          type: "clickhouse",
          config: %{url: "http://localhost:8123", database: "default", port: 8123},
          default_ingest?: true
        })
        |> json_response(201)

      assert response["name"] == name
      assert response["default_ingest?"] == true
    end

    test "creates a backend without `default_ingest?` field (defaults to false)", %{
      conn: conn,
      user: user
    } do
      name = TestUtils.random_string()

      response =
        conn
        |> add_access_token(user, "private")
        |> post("/api/backends", %{
          name: name,
          type: "clickhouse",
          config: %{url: "http://localhost:8123", database: "default", port: 8123}
        })
        |> json_response(201)

      assert response["name"] == name
      assert response["default_ingest?"] == false
    end
  end

  describe "update/2" do
    test "updates an existing backend from a user", %{
      conn: conn,
      user: user
    } do
      backend = insert(:backend, user: user)
      name = TestUtils.random_string()

      response =
        conn
        |> add_access_token(user, "private")
        |> patch("/api/backends/#{backend.token}", %{name: name})
        |> response(204)

      assert response == ""
    end

    test "returns not found if doesn't own the resource", %{conn: conn, user: user} do
      invalid_user = insert(:user)
      backend = insert(:backend, user: user)

      conn
      |> add_access_token(invalid_user, "private")
      |> patch("/api/backends/#{backend.token}", %{name: TestUtils.random_string()})
      |> response(404)
    end

    test "team member cannot update a team-owned backend", %{conn: conn} do
      member = insert(:user)
      team_user = insert(:team_user, email: member.email)
      backend = insert(:backend, user: team_user.team.user)

      assert conn
             |> add_access_token(member, "private")
             |> patch("/api/backends/#{backend.token}", %{name: "updated"})
             |> response(404)
    end

    test "cannot transfer backend ownership via user_id param", %{conn: conn, user: user} do
      backend = insert(:backend, user: user)
      victim = insert(:user)

      conn
      |> add_access_token(user, "private")
      |> patch("/api/backends/#{backend.token}", %{user_id: victim.id})
      |> response(204)

      assert Logflare.Backends.get_backend(backend.id).user_id == user.id
    end

    test "returns 422 on bad arguments", %{conn: conn, user: user} do
      backend = insert(:backend, user: user)

      resp =
        conn
        |> add_access_token(user, "private")
        |> patch("/api/backends/#{backend.token}", %{name: 123})
        |> json_response(422)

      assert resp == %{"errors" => %{"name" => ["is invalid"]}}
    end

    test "updates `default_ingest?` field", %{conn: conn, user: user} do
      backend = insert(:backend, user: user, type: :bigquery, default_ingest?: false)
      source = insert(:source, user: user, default_ingest_backend_enabled?: true)

      conn
      |> add_access_token(user, "private")
      |> patch("/api/backends/#{backend.token}", %{default_ingest?: true, source_id: source.id})
      |> response(204)

      response =
        conn
        |> add_access_token(user, "private")
        |> get("/api/backends/#{backend.token}")
        |> json_response(200)

      assert response["default_ingest?"] == true

      updated_source = Logflare.Sources.get(source.id) |> Logflare.Sources.preload_backends()
      assert Enum.any?(updated_source.backends, &(&1.id == backend.id))
    end

    test "returns error when enabling default_ingest? without source_id", %{
      conn: conn,
      user: user
    } do
      backend = insert(:backend, user: user, type: :bigquery, default_ingest?: false)

      response =
        conn
        |> add_access_token(user, "private")
        |> patch("/api/backends/#{backend.token}", %{default_ingest?: true})
        |> json_response(422)

      assert response == %{
               "errors" => %{
                 "default_ingest?" => ["Please select a source when enabling default ingest"]
               }
             }
    end

    test "partial config update preserves existing fields", %{conn: conn, user: user} do
      backend =
        insert(:backend,
          user: user,
          type: :webhook,
          config: %{url: "http://example.com", gzip: true, http: "http2"}
        )

      conn
      |> add_access_token(user, "private")
      |> patch("/api/backends/#{backend.token}", %{config: %{gzip: false, http: "http1"}})
      |> response(204)

      response =
        conn
        |> add_access_token(user, "private")
        |> get("/api/backends/#{backend.token}")
        |> json_response(200)

      assert response["config"]["url"] == "http://example.com"
      assert response["config"]["gzip"] == false
      assert response["config"]["http"] == "http1"
    end

    test "redacted webhook config round-trip preserves stored credentials", %{
      conn: conn,
      user: user
    } do
      original_url = "https://user:url-secret@example.com/hooks"
      original_header = "header-secret"

      backend =
        insert(:backend,
          type: :webhook,
          user: user,
          config: %{
            url: original_url,
            headers: %{"X-Webhook-Secret" => original_header}
          }
        )

      redacted_config =
        conn
        |> add_access_token(user, "private")
        |> get("/api/backends/#{backend.token}")
        |> json_response(200)
        |> Map.fetch!("config")

      conn
      |> add_access_token(user, "private")
      |> patch("/api/backends/#{backend.token}", %{config: redacted_config})
      |> response(204)

      stored_config = Logflare.Backends.get_backend(backend.id).config_encrypted
      stored_url = Map.get(stored_config, :url) || Map.get(stored_config, "url")
      stored_headers = Map.get(stored_config, :headers) || Map.get(stored_config, "headers")

      assert stored_url == original_url
      assert Map.get(stored_headers, "x-webhook-secret") == original_header
    end
  end

  describe "delete/2" do
    test "deletes an existing source from a user", %{
      conn: conn,
      user: user
    } do
      name = TestUtils.random_string()
      backend = insert(:backend, user: user)

      assert conn
             |> add_access_token(user, "private")
             |> delete("/api/backends/#{backend.token}", %{name: name})
             |> response(204)

      assert conn
             |> add_access_token(user, "private")
             |> get("/api/backends/#{backend.token}")
             |> response(404)
    end

    test "returns not found if doesn't own the source", %{
      conn: conn,
      user: user
    } do
      invalid_user = insert(:user)
      backend = insert(:backend, user: user)

      assert conn
             |> add_access_token(invalid_user, "private")
             |> delete("/api/backends/#{backend.token}")
             |> response(404)
    end

    test "team member cannot delete a team-owned backend", %{conn: conn} do
      member = insert(:user)
      team_user = insert(:team_user, email: member.email)
      backend = insert(:backend, user: team_user.team.user)

      assert conn
             |> add_access_token(member, "private")
             |> delete("/api/backends/#{backend.token}")
             |> response(404)
    end
  end

  describe "test_connection/2" do
    test "returns 200 if connection is successful", %{conn: conn, user: user} do
      backend = insert(:backend, user: user, type: :axiom)

      Logflare.Backends.Adaptor.AxiomAdaptor
      |> Mimic.expect(:test_connection, fn %Logflare.Backends.Backend{id: id} ->
        assert id == backend.id
        :ok
      end)

      response =
        conn
        |> add_access_token(user, "private")
        |> post("/api/backends/#{backend.token}/test")
        |> json_response(200)

      assert response == %{"connected?" => true}
    end

    test "returns 400 if connection fails", %{conn: conn, user: user} do
      backend = insert(:backend, user: user)

      Logflare.Backends
      |> Mimic.expect(:test_connection, fn _ -> {:error, :some_reason} end)

      response =
        conn
        |> add_access_token(user, "private")
        |> post("/api/backends/#{backend.token}/test")
        |> json_response(200)

      assert response == %{"connected?" => false, "reason" => "some_reason"}
    end

    test "returns 404 if backend doesn't exist or doesn't belong to user", %{
      conn: conn,
      user: user
    } do
      backend = insert(:backend)

      conn
      |> add_access_token(user, "private")
      |> post("/api/backends/#{backend.token}/test")
      |> response(404)
    end

    test "team member cannot test a team-owned backend connection", %{conn: conn} do
      member = insert(:user)
      team_user = insert(:team_user, email: member.email)
      backend = insert(:backend, user: team_user.team.user)

      assert conn
             |> add_access_token(member, "private")
             |> post("/api/backends/#{backend.token}/test")
             |> response(404)
    end
  end
end
