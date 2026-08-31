defmodule LogflareWeb.Api.IngestSourceControllerTest do
  use LogflareWeb.ConnCase

  import Ecto.Query, only: [from: 2]

  alias Logflare.Sources.Source

  setup do
    user = insert(:user)
    other_user = insert(:user)
    source_a = insert(:source, user: user, name: "Alpha")
    source_b = insert(:source, user: user, name: "Bravo")
    other_source = insert(:source, user: other_user, name: "Other")

    {:ok, user: user, source_a: source_a, source_b: source_b, other_source: other_source}
  end

  describe "index/2" do
    test "lists only token and name for account-wide ingest credentials in stable order", %{
      conn: conn,
      user: user,
      source_a: source_a,
      source_b: source_b
    } do
      json_conn =
        conn
        |> add_access_token(user, "ingest")
        |> get("/api/ingest-sources")

      assert json_response(json_conn, 200) == [
               %{"token" => to_string(source_a.token), "name" => source_a.name},
               %{"token" => to_string(source_b.token), "name" => source_b.name}
             ]

      assert get_resp_header(json_conn, "cache-control") == ["no-store"]
    end

    test "lists all owned sources for private and legacy API key credentials", %{
      conn: conn,
      user: user
    } do
      for credential <- [
            fn conn -> add_access_token(conn, user, "private") end,
            fn conn -> put_req_header(conn, "x-api-key", user.api_key) end
          ] do
        assert 2 ==
                 conn
                 |> credential.()
                 |> get("/api/ingest-sources")
                 |> json_response(200)
                 |> length()
      end
    end

    test "limits source and deprecated collection credentials to their owned sources", %{
      conn: conn,
      user: user,
      source_a: source_a,
      source_b: source_b,
      other_source: other_source
    } do
      response =
        conn
        |> add_access_token(
          user,
          "ingest:source:#{source_a.id} ingest:collection:#{source_b.id} ingest:source:#{other_source.id}"
        )
        |> get("/api/ingest-sources")
        |> json_response(200)

      assert response == [
               %{"token" => to_string(source_a.token), "name" => source_a.name},
               %{"token" => to_string(source_b.token), "name" => source_b.name}
             ]
    end

    test "returns an empty list for a scope that names another user's source", %{
      conn: conn,
      user: user,
      other_source: other_source
    } do
      assert [] =
               conn
               |> add_access_token(user, "ingest:source:#{other_source.id}")
               |> get("/api/ingest-sources")
               |> json_response(200)
    end

    test "excludes system sources from JSON and CSV discovery", %{conn: conn, user: user} do
      system_source = insert(:source, user: user, name: "system.logs", system_source: true)

      response =
        conn
        |> add_access_token(user, "ingest")
        |> get("/api/ingest-sources")
        |> json_response(200)

      refute Enum.any?(response, &(&1["token"] == to_string(system_source.token)))

      csv_conn =
        build_conn()
        |> add_access_token(user, "ingest:source:#{system_source.id}")
        |> put_req_header("accept", "text/csv")
        |> get("/api/ingest-sources")

      assert response(csv_conn, 200) == "token,name\r\n"
    end

    test "preserves deprecated empty and public ingest-compatible access token scopes", %{
      conn: conn,
      user: user
    } do
      for scopes <- ["", "public"] do
        assert 2 ==
                 conn
                 |> add_access_token(user, scopes)
                 |> get("/api/ingest-sources")
                 |> json_response(200)
                 |> length()
      end
    end

    test "rejects missing, invalid, query-only, and query-only parameter credentials", %{
      conn: conn,
      user: user
    } do
      query_token = Logflare.Auth.create_access_token(user, %{scopes: "query"}) |> elem(1)

      for request <- [
            fn conn -> get(conn, "/api/ingest-sources") end,
            fn conn ->
              conn
              |> put_req_header("authorization", "Bearer invalid")
              |> get("/api/ingest-sources")
            end,
            fn conn -> conn |> add_access_token(user, "query") |> get("/api/ingest-sources") end,
            fn conn -> get(conn, "/api/ingest-sources?api_key=#{query_token.token}") end
          ] do
        assert %{"error" => "Unauthorized"} = conn |> request.() |> json_response(401)
      end
    end

    test "returns empty JSON and header-only CSV results", %{conn: conn} do
      user = insert(:user)

      assert [] =
               conn
               |> add_access_token(user, "ingest")
               |> get("/api/ingest-sources")
               |> json_response(200)

      csv_conn =
        build_conn()
        |> add_access_token(user, "ingest")
        |> put_req_header("accept", "text/csv")
        |> get("/api/ingest-sources")

      assert response(csv_conn, 200) == "token,name\r\n"
      assert get_resp_header(csv_conn, "content-type") == ["text/csv; charset=utf-8"]
    end

    test "returns RFC 4180 CSV with no-store caching", %{
      conn: conn,
      user: user,
      source_a: source_a
    } do
      name = "comma, \"quote\"\nline"

      Logflare.Repo.update_all(
        from(source in Source, where: source.id == ^source_a.id),
        set: [name: name]
      )

      csv_conn =
        conn
        |> add_access_token(user, "ingest")
        |> put_req_header("accept", "text/csv")
        |> get("/api/ingest-sources")

      assert response(csv_conn, 200) =~ "\"comma, \"\"quote\"\"\nline\""
      assert get_resp_header(csv_conn, "content-type") == ["text/csv; charset=utf-8"]
      assert get_resp_header(csv_conn, "cache-control") == ["no-store"]
    end
  end
end
