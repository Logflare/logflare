defmodule LogflareWeb.Api.RuleControllerTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  setup %{conn: conn} do
    insert(:plan, name: "Free")
    user = insert(:user)
    conn = add_access_token(conn, user, "private")
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)
    {:ok, conn: conn, user: user, backend: backend, source: source}
  end

  test "list rules", %{conn: conn, source: source, backend: backend, user: user} do
    insert(:rule, backend: insert(:backend, user: user), source: source)
    rule = insert(:rule, backend: backend, source: source)

    assert [result] =
             conn
             |> get(~p"/api/rules?#{%{backend_token: backend.token}}")
             |> json_response(200)

    assert result["id"] == rule.id
    assert result["token"] == rule.token
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
