defmodule LogflareWeb.Api.RuleControllerTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  alias LogflareWeb.Api.RuleController
  alias OpenApiSpex.Parameter
  alias OpenApiSpex.Schema

  setup %{conn: conn} do
    insert(:plan, name: "Free")
    user = insert(:user)
    conn = add_access_token(conn, user, "private")
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)
    {:ok, conn: conn, user: user, backend: backend, source: source}
  end

  test "lists rules owned by the authenticated user", %{
    conn: conn,
    source: source,
    backend: backend,
    user: user
  } do
    first_rule = insert(:rule, backend: backend, source: source)
    second_rule = insert(:rule, backend: insert(:backend, user: user), source: source)

    other_user = insert(:user)

    insert(:rule,
      backend: insert(:backend, user: other_user),
      source: insert(:source, user: other_user)
    )

    response =
      conn
      |> get(~p"/api/rules")
      |> json_response(200)

    assert MapSet.new(Enum.map(response, & &1["id"])) ==
             MapSet.new([first_rule.id, second_rule.id])
  end

  test "filters rules by backend ID or token", %{
    conn: conn,
    source: source,
    backend: backend,
    user: user
  } do
    rule = insert(:rule, backend: backend, source: source)
    insert(:rule, backend: insert(:backend, user: user), source: source)

    for filters <- [%{backend_id: backend.id}, %{backend_token: backend.token}] do
      assert [result] =
               conn
               |> get(~p"/api/rules?#{filters}")
               |> json_response(200)

      assert result["id"] == rule.id
      assert result["token"] == rule.token
    end
  end

  test "does not list rules for an unauthorized backend", %{conn: conn} do
    other_user = insert(:user)
    other_backend = insert(:backend, user: other_user)
    other_source = insert(:source, user: other_user)
    insert(:rule, backend: other_backend, source: other_source)

    assert %{"error" => "Not Found"} =
             conn
             |> get(~p"/api/rules?#{%{backend_id: other_backend.id}}")
             |> json_response(404)
  end

  test "list rules requires a private token", %{conn: conn, user: user} do
    assert %{"error" => "Unauthorized"} =
             conn
             |> add_access_token(user, "public")
             |> get(~p"/api/rules")
             |> json_response(401)
  end

  test "documents optional backend filters for rule discovery" do
    parameters =
      RuleController.open_api_operation(:index).parameters
      |> Map.new(&{&1.name, &1})

    assert %{
             backend_id: %Parameter{
               in: :query,
               description: "Optional backend ID to filter the rule list",
               required: false,
               schema: %Schema{type: :integer}
             },
             backend_token: %Parameter{
               in: :query,
               description: "Optional backend UUID to filter the rule list",
               required: false,
               schema: %Schema{type: :string}
             }
           } = parameters
  end

  test "show rules", %{conn: conn, source: source, backend: backend} do
    rule = insert(:rule, backend: backend, source: source)
    insert_pair(:rule, backend: backend, source: source)

    assert result =
             conn
             |> get(~p"/api/rules/#{rule.token}")
             |> json_response(200)

    assert result["id"] == rule.id
    assert result["token"] == rule.token
    assert result["lql_string"] == rule.lql_string
  end

  describe "with rules" do
    test "create rule", %{conn: conn, backend: backend, source: source} do
      assert %{"token" => rule_token} =
               conn
               |> post(~p"/api/rules", %{
                 source_id: source.id,
                 backend_id: backend.id,
                 lql_string: "test"
               })
               |> json_response(201)

      assert rule_token

      assert conn
             |> get(~p"/api/rules/#{rule_token}")
             |> json_response(200)
    end

    test "create multiple rules", %{conn: conn, backend: backend, source: source} do
      assert %{"results" => [_]} =
               conn
               |> Plug.Conn.put_req_header("content-type", "application/json")
               |> post(
                 ~p"/api/rules",
                 Jason.encode!([
                   %{
                     source_id: source.id,
                     backend_id: backend.id,
                     lql_string: "test"
                   }
                 ])
               )
               |> json_response(201)
    end

    test "returns 422 when a single rule is invalid", %{
      conn: conn,
      backend: backend,
      source: source
    } do
      assert %{"errors" => %{"lql_string" => _}} =
               conn
               |> post(~p"/api/rules", %{
                 source_id: source.id,
                 backend_id: backend.id,
                 lql_string: ""
               })
               |> json_response(422)
    end

    test "create rule with bad user", %{conn: conn, backend: backend, source: source} do
      user = insert(:user)

      assert %{"error" => _} =
               conn
               |> add_access_token(user, "private")
               |> post(~p"/api/rules", %{
                 source_id: source.id,
                 backend_id: backend.id,
                 lql_string: "test"
               })
               |> json_response(404)
    end

    test "create multiple rules error", %{conn: conn, backend: backend, source: source} do
      assert %{"errors" => _} =
               conn
               |> Plug.Conn.put_req_header("content-type", "application/json")
               |> post(
                 ~p"/api/rules",
                 Jason.encode!([
                   %{
                     source_id: source.id,
                     backend_id: backend.id
                   }
                 ])
               )
               |> json_response(400)
    end

    test "update rule", %{conn: conn, backend: backend, source: source} do
      rule = insert(:rule, lql_string: "initial", source: source, backend: backend)

      assert %{"token" => rule_token} =
               conn
               |> put(~p"/api/rules/#{rule.token}", %{
                 lql_string: "adjusted"
               })
               |> json_response(200)

      assert %{"lql_string" => "adjusted"} =
               conn
               |> get(~p"/api/rules/#{rule_token}")
               |> json_response(200)
    end

    test "returns 422 when updating a rule with an invalid LQL expression", %{
      conn: conn,
      backend: backend,
      source: source
    } do
      rule = insert(:rule, lql_string: "initial", source: source, backend: backend)

      assert %{"errors" => %{"lql_string" => _}} =
               conn
               |> put(~p"/api/rules/#{rule.token}", %{lql_string: ""})
               |> json_response(422)
    end

    test "attacker cannot repoint their rule to a victim's source or backend", %{
      conn: conn,
      backend: backend,
      source: source
    } do
      rule = insert(:rule, lql_string: "initial", source: source, backend: backend)

      victim = insert(:user)
      victim_source = insert(:source, user: victim)
      victim_backend = insert(:backend, user: victim)

      conn
      |> put(~p"/api/rules/#{rule.token}", %{
        source_id: victim_source.id,
        backend_id: victim_backend.id
      })
      |> json_response(404)

      reloaded = Logflare.Rules.get_rule(rule.id)
      assert reloaded.source_id == source.id
      assert reloaded.backend_id == backend.id
    end

    test "update rule with bad user", %{conn: conn, backend: backend, source: source} do
      user = insert(:user)
      rule = insert(:rule, lql_string: "initial", source: source, backend: backend)

      assert %{"error" => _} =
               conn
               |> add_access_token(user, "private")
               |> put(~p"/api/rules/#{rule.token}", %{
                 lql_string: "adjusted"
               })
               |> json_response(404)

      assert %{"lql_string" => "initial"} =
               conn
               |> get(~p"/api/rules/#{rule.token}")
               |> json_response(200)
    end

    test "delete rule", %{conn: conn, backend: backend, source: source} do
      rule = insert(:rule, backend: backend, source: source)

      assert conn
             |> delete(~p"/api/rules/#{rule.token}")
             |> response(204)

      assert conn
             |> get(~p"/api/rules/#{rule.token}")
             |> json_response(404)
    end

    test "delete rule with bad user", %{conn: conn, backend: backend, source: source} do
      user = insert(:user)
      rule = insert(:rule, backend: backend, source: source)

      assert conn
             |> add_access_token(user, "private")
             |> delete(~p"/api/rules/#{rule.token}")
             |> response(404)

      assert conn
             |> get(~p"/api/rules/#{rule.token}")
             |> json_response(200)
    end
  end
end
