defmodule LogflareWeb.Api.EndpointControllerTest do
  use LogflareWeb.ConnCase
  alias Logflare.Endpoints

  setup do
    insert(:plan)
    endpoints = insert_list(2, :endpoint)
    user = insert(:user, endpoint_queries: endpoints)
    insert(:source, name: "logs", user: user)

    {:ok, user: user, endpoints: endpoints}
  end

  describe "index/2" do
    test "returns list of endpoint queries for given user", %{
      conn: conn,
      user: user,
      endpoints: endpoints
    } do
      response =
        conn
        |> add_access_token(user, ~w(private))
        |> get(~p"/api/endpoints")
        |> json_response(200)

      response = response |> Enum.map(& &1["token"]) |> Enum.sort()
      expected = endpoints |> Enum.map(& &1.token) |> Enum.sort()

      assert response == expected
    end
  end

  describe "team access" do
    test "team member can manage team-owned endpoints and attach a team-owned backend", %{
      conn: conn
    } do
      member = insert(:user, endpoints_beta: true)
      team_user = insert(:team_user, email: member.email)
      owner = team_user.team.user
      managed_endpoint = insert(:endpoint, user: owner, name: "initial")
      deleted_endpoint = insert(:endpoint, user: owner)
      unrelated_endpoint = insert(:endpoint)
      backend = insert(:backend, user: owner)
      conn = add_access_token(conn, member, "private")

      response =
        conn
        |> get(~p"/api/endpoints")
        |> json_response(200)

      assert MapSet.new(Enum.map(response, & &1["token"])) ==
               MapSet.new([managed_endpoint.token, deleted_endpoint.token])

      assert %{"token" => managed_token} =
               conn
               |> get(~p"/api/endpoints/#{managed_endpoint.token}")
               |> json_response(200)

      assert managed_token == managed_endpoint.token

      assert conn
             |> get(~p"/api/endpoints/#{unrelated_endpoint.token}")
             |> response(404)

      assert conn
             |> patch(~p"/api/endpoints/#{managed_endpoint.token}", %{name: "updated"})
             |> text_response(204) == ""

      assert conn
             |> patch(~p"/api/endpoints/#{managed_endpoint.token}", %{backend_id: backend.id})
             |> text_response(204) == ""

      assert %{backend_id: backend_id, name: "updated"} =
               Endpoints.get_endpoint_query(managed_endpoint.id)

      assert backend_id == backend.id

      assert conn
             |> delete(~p"/api/endpoints/#{deleted_endpoint.token}")
             |> text_response(204) == ""
    end
  end

  describe "show/2" do
    test "returns single endpoint query for given user", %{
      conn: conn,
      user: user,
      endpoints: [endpoint | _]
    } do
      response =
        conn
        |> add_access_token(user, ~w(private))
        |> get(~p"/api/endpoints/#{endpoint.token}")
        |> json_response(200)

      assert response["token"] == endpoint.token
    end

    test "returns not found if doesn't own the endpoint query", %{
      conn: conn,
      endpoints: [endpoint | _]
    } do
      invalid_user = insert(:user)

      conn
      |> add_access_token(invalid_user, ~w(private))
      |> get(~p"/api/endpoints/#{endpoint.token}")
      |> json_response(404)
    end
  end

  describe "create/2" do
    test "creates a new endpoint query for an authenticated user", %{
      conn: conn,
      user: user
    } do
      name = TestUtils.random_string()
      query = "select a from logs"

      response =
        conn
        |> add_access_token(user, ~w(private))
        |> post(~p"/api/endpoints", %{name: name, language: "bq_sql", query: query})
        |> json_response(201)

      assert response["name"] == name
      assert response["query"] == query

      assert [version] = PaperTrail.get_versions(Endpoints.EndpointQuery, response["id"])
      assert version.origin =~ "API: id"
    end

    test "returns 422 on missing arguments", %{conn: conn, user: user} do
      response =
        conn
        |> add_access_token(user, ~w(private))
        |> post(~p"/api/endpoints")
        |> json_response(422)

      assert %{"errors" => %{"name" => _, "query" => _}} = response
    end

    test "returns 422 on bad arguments", %{conn: conn, user: user} do
      response =
        conn
        |> add_access_token(user, ~w(private))
        |> post(~p"/api/endpoints", %{name: 123})
        |> json_response(422)

      assert %{"errors" => %{"name" => _, "query" => _}} = response
    end

    test "attacker cannot create an endpoint with another user's backend", %{conn: conn} do
      attacker = insert(:user, endpoints_beta: true)
      victim = insert(:user)

      victim_backend =
        insert(:backend,
          user: victim,
          type: :postgres,
          config: %{url: "postgresql://victim.local/logflare"}
        )

      assert conn
             |> add_access_token(attacker, "private")
             |> post(~p"/api/endpoints", %{
               name: "test",
               language: "pg_sql",
               query: "select 1",
               backend_id: victim_backend.id
             })
             |> json_response(404)
    end
  end

  describe "update/2" do
    test "updates an existing endpoint query from a user", %{
      conn: conn,
      user: user,
      endpoints: [endpoint | _]
    } do
      name = TestUtils.random_string()
      token = endpoint.token

      initial_versions = PaperTrail.get_versions(Endpoints.EndpointQuery, endpoint.id)

      response =
        conn
        |> add_access_token(user, ~w(private))
        |> patch(~p"/api/endpoints/#{token}", %{name: name})
        |> text_response(204)

      assert response == ""

      another_name = TestUtils.random_string()

      response =
        conn
        |> add_access_token(user, ~w(private))
        |> put(~p"/api/endpoints/#{token}", %{name: another_name})
        |> json_response(200)

      assert %{"name" => ^another_name, "token" => ^token} = response

      assert_in_delta length(initial_versions),
                      length(PaperTrail.get_versions(Endpoints.EndpointQuery, endpoint.id)),
                      2
    end

    test "returns not found if doesn't own the endpoint query", %{
      conn: conn,
      endpoints: [endpoint | _]
    } do
      invalid_user = insert(:user)

      conn
      |> add_access_token(invalid_user, ~w(private))
      |> patch(~p"/api/endpoints/#{endpoint.token}", %{name: TestUtils.random_string()})
      |> json_response(404)
    end

    test "returns 422 on bad arguments", %{
      conn: conn,
      user: user,
      endpoints: [endpoint | _]
    } do
      response =
        conn
        |> add_access_token(user, ~w(private))
        |> patch(~p"/api/endpoints/#{endpoint.token}", %{name: 123})
        |> json_response(422)

      assert %{"errors" => %{"name" => ["is invalid"]}} = response
    end

    test "attacker cannot update their endpoint to use another user's backend", %{conn: conn} do
      attacker = insert(:user, endpoints_beta: true)
      victim = insert(:user)
      endpoint = insert(:endpoint, user: attacker)

      victim_backend =
        insert(:backend,
          user: victim,
          type: :postgres,
          config: %{url: "postgresql://victim.local/logflare"}
        )

      conn
      |> add_access_token(attacker, "private")
      |> patch(~p"/api/endpoints/#{endpoint.token}", %{backend_id: victim_backend.id})

      assert %{backend_id: backend_id} = Logflare.Endpoints.get_endpoint_query(endpoint.id)
      refute backend_id == victim_backend.id
    end
  end

  describe "delete/2" do
    test "deletes an existing endpoint query from a user", %{
      conn: conn,
      user: user,
      endpoints: [endpoint | _]
    } do
      name = TestUtils.random_string()

      conn
      |> add_access_token(user, ~w(private))
      |> delete(~p"/api/endpoints/#{endpoint.token}", %{name: name})
      |> text_response(204)

      conn
      |> add_access_token(user, ~w(private))
      |> get(~p"/api/endpoints/#{endpoint.token}")
      |> json_response(404)

      assert [%_{meta: meta, event: "delete"}] =
               PaperTrail.get_versions(Endpoints.EndpointQuery, endpoint.id)

      assert meta["endpoint_snapshot"]["query"] == endpoint.query
    end

    test "returns not found if doesn't own the endpoint query", %{
      conn: conn,
      endpoints: [endpoint | _]
    } do
      invalid_user = insert(:user)

      conn
      |> add_access_token(invalid_user, ~w(private))
      |> delete(~p"/api/endpoints/#{endpoint.token}", %{name: TestUtils.random_string()})
      |> json_response(404)
    end
  end
end
