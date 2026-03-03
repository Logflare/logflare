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
                 "username" => "test_user",
                 "password" => "REDACTED",
                 "database" => "default",
                 "port" => 8123,
                 "pool_size" => 10
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
  end
end
