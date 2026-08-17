defmodule LogflareWeb.Api.BackendControllerTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  alias Logflare.Backends.Backend
  alias LogflareWeb.ApiSpec
  alias LogflareWeb.OpenApiSchemas.BackendResponseConfigSchema

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

    test "every mapped backend response exposes only its safe response config", %{
      conn: conn,
      user: user
    } do
      backends =
        for type <- Map.keys(Backend.adaptor_mapping()) do
          insert(:backend,
            user: user,
            type: type,
            config_encrypted: response_test_config(type),
            metadata: %{secret: "synthetic-secret"}
          )
        end

      responses =
        conn
        |> add_access_token(user, "private")
        |> get(~p"/api/backends")
        |> json_response(200)
        |> Map.new(&{&1["id"], &1})

      api_spec = ApiSpec.spec()

      response_config_schema =
        Map.fetch!(api_spec.components.schemas, BackendResponseConfigSchema.schema().title)

      for backend <- backends do
        response = responses[backend.id]

        assert response["config"] == expected_response_config(backend.type)

        assert {:ok, _config} =
                 OpenApiSpex.cast_value(response["config"], response_config_schema, api_spec)

        assert {:error, _reason} =
                 response["config"]
                 |> Map.put("unknown_secret", "synthetic-secret")
                 |> OpenApiSpex.cast_value(response_config_schema, api_spec)

        refute Jason.encode!(response) =~ "synthetic-secret"
        refute Map.has_key?(response, "metadata")
        refute Map.has_key?(response["config"], "headers")
      end
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
  end

  describe "show/2" do
    test "returns single backend for given user", %{conn: conn, user: user} do
      backend = insert(:backend, user: user)

      response =
        conn
        |> add_access_token(user, "private")
        |> get("/api/backends/#{backend.token}")
        |> json_response(200)

      assert response["id"] == backend.id
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
                 "schema" => "_my_schema"
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
            pool_size: 10
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
                 "database" => "default",
                 "port" => 8123,
                 "pool_size" => 10
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
                 "region" => "US1"
               }
             } = json_response(conn, 201)
    end

    test "creates an elastic backend without returning credentials or metadata", %{
      conn: conn,
      user: user
    } do
      name = TestUtils.random_string()

      response =
        conn
        |> add_access_token(user, "private")
        |> post("/api/backends", %{
          name: name,
          type: "elastic",
          config: %{url: "https://example.com", username: "someuser", password: "12345"},
          metadata: %{some: "data"}
        })
        |> json_response(201)

      assert response["config"] == %{"url" => "https://example.com"}
      refute Map.has_key?(response, "metadata")
    end

    test "creates a loki backend without returning credentials or metadata", %{
      conn: conn,
      user: user
    } do
      name = TestUtils.random_string()

      response =
        conn
        |> add_access_token(user, "private")
        |> post("/api/backends", %{
          name: name,
          type: "loki",
          config: %{url: "https://example.com", username: "someuser", password: "12345"},
          metadata: %{some: "data"}
        })
        |> json_response(201)

      assert response["config"] == %{"url" => "https://example.com"}
      refute Map.has_key?(response, "metadata")
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

  defp response_test_config(:webhook),
    do: %{
      url: "https://example.com",
      http: "http2",
      gzip: true,
      headers: %{"X-Key" => "synthetic-secret"}
    }

  defp response_test_config(:elastic),
    do: %{url: "https://example.com", username: "synthetic-user", password: "synthetic-secret"}

  defp response_test_config(:datadog), do: %{region: "US1", api_key: "synthetic-secret"}
  defp response_test_config(:sentry), do: %{dsn: "https://key:synthetic-secret@example.com/1"}

  defp response_test_config(:postgres),
    do: %{
      hostname: "db.example.com",
      database: "logs",
      schema: "public",
      port: 5432,
      pool_size: 2,
      username: "synthetic-user",
      password: "synthetic-secret"
    }

  defp response_test_config(:bigquery),
    do: %{project_id: "project-id", dataset_id: "dataset", arbitrary: "synthetic-secret"}

  defp response_test_config(:loki),
    do: %{
      url: "https://example.com",
      username: "synthetic-user",
      password: "synthetic-secret",
      headers: %{"X-Key" => "synthetic-secret"}
    }

  defp response_test_config(:clickhouse),
    do: %{
      url: "https://example.com",
      database: "default",
      port: 8123,
      pool_size: 2,
      username: "synthetic-user",
      password: "synthetic-secret",
      read_only_url: "https://legacy-read.example.com",
      read_only_urls: %{
        "primary" => "https://read.example.com",
        "secondary" => "https://read-2.example.com"
      },
      default_read_cluster: "primary",
      use_async_inserts_for_small_batches: false,
      async_insert_cluster_url: "https://async.example.com",
      async_insert_max_rows: 1000,
      max_event_age_hours: 72
    }

  defp response_test_config(:incidentio),
    do: %{
      api_token: "synthetic-secret",
      alert_source_config_id: "source",
      metadata: %{secret: "synthetic-secret"}
    }

  defp response_test_config(:s3),
    do: %{
      s3_bucket: "bucket",
      storage_region: "us-east-1",
      batch_timeout: 1000,
      endpoint: "https://s3.example.com",
      access_key_id: "synthetic-user",
      secret_access_key: "synthetic-secret"
    }

  defp response_test_config(:axiom),
    do: %{domain: "api.axiom.co", dataset_name: "logs", api_token: "synthetic-secret"}

  defp response_test_config(:otlp),
    do: %{
      endpoint: "https://example.com",
      protocol: "http/protobuf",
      gzip: true,
      flatten_to_attributes: true,
      headers: %{"X-Key" => "synthetic-secret"}
    }

  defp response_test_config(:last9),
    do: %{region: "US-WEST-1", username: "synthetic-user", password: "synthetic-secret"}

  defp response_test_config(:syslog),
    do: %{
      host: "syslog.example.com",
      port: 6514,
      tls: true,
      structured_data: "[credential@1 token=\"synthetic-secret\"]",
      max_message_bytes: 1000,
      cipher_key: "synthetic-secret",
      ca_cert: "synthetic-secret",
      client_cert: "synthetic-secret",
      client_key: "synthetic-secret"
    }

  defp expected_response_config(:webhook),
    do: %{"url" => "https://example.com", "http" => "http2", "gzip" => true}

  defp expected_response_config(:elastic), do: %{"url" => "https://example.com"}
  defp expected_response_config(:datadog), do: %{"region" => "US1"}
  defp expected_response_config(:sentry), do: %{}

  defp expected_response_config(:postgres),
    do: %{
      "hostname" => "db.example.com",
      "database" => "logs",
      "schema" => "public",
      "port" => 5432,
      "pool_size" => 2
    }

  defp expected_response_config(:bigquery),
    do: %{"project_id" => "project-id", "dataset_id" => "dataset"}

  defp expected_response_config(:loki), do: %{"url" => "https://example.com"}

  defp expected_response_config(:clickhouse),
    do: %{
      "url" => "https://example.com",
      "database" => "default",
      "port" => 8123,
      "pool_size" => 2,
      "read_only_url" => "https://legacy-read.example.com",
      "read_only_urls" => %{
        "primary" => "https://read.example.com",
        "secondary" => "https://read-2.example.com"
      },
      "default_read_cluster" => "primary",
      "use_async_inserts_for_small_batches" => false,
      "async_insert_cluster_url" => "https://async.example.com",
      "async_insert_max_rows" => 1000,
      "max_event_age_hours" => 72
    }

  defp expected_response_config(:incidentio), do: %{}

  defp expected_response_config(:s3),
    do: %{
      "s3_bucket" => "bucket",
      "storage_region" => "us-east-1",
      "batch_timeout" => 1000,
      "endpoint" => "https://s3.example.com"
    }

  defp expected_response_config(:axiom),
    do: %{"domain" => "api.axiom.co", "dataset_name" => "logs"}

  defp expected_response_config(:otlp),
    do: %{
      "endpoint" => "https://example.com",
      "protocol" => "http/protobuf",
      "gzip" => true,
      "flatten_to_attributes" => true
    }

  defp expected_response_config(:last9), do: %{"region" => "US-WEST-1"}

  defp expected_response_config(:syslog),
    do: %{
      "host" => "syslog.example.com",
      "port" => 6514,
      "tls" => true,
      "max_message_bytes" => 1000
    }
end
