defmodule LogflareWeb.Api.SourceDiscoveryControllerTest do
  use LogflareWeb.ConnCase

  alias Logflare.Auth
  alias LogflareWeb.Api.SourceCsv

  setup do
    insert(:plan, name: "Free")
    user = insert(:user)
    alpha = insert(:source, user: user, name: "Alpha")
    beta = insert(:source, user: user, name: "Beta")
    foreign = insert(:source, user: insert(:user), name: "Foreign")

    {:ok, user: user, alpha: alpha, beta: beta, foreign: foreign}
  end

  test "private JSON preserves the full dashboard source representation and cache policy", %{
    conn: conn,
    user: user,
    alpha: alpha
  } do
    system_source = insert(:source, user: user, name: "system.logs", system_source: true)
    conn = conn |> add_access_token(user, "private") |> get("/api/sources")

    sources = json_response(conn, 200)

    assert [source | _] = sources
    assert source["token"] == Atom.to_string(alpha.token)
    assert Map.has_key?(source, "id")
    assert Map.has_key?(source, "metrics")
    assert Enum.any?(sources, &(&1["token"] == Atom.to_string(system_source.token)))
    assert get_resp_header(conn, "cache-control") == ["no-store"]
  end

  test "partner impersonation retains the full JSON response", %{conn: conn} do
    partner = insert(:partner)
    user = insert(:user, partner: partner)
    source = insert(:source, user: user)

    conn =
      conn
      |> put_req_header("x-lf-partner-user", user.token)
      |> add_partner_access_token(partner)
      |> get("/api/sources")

    assert [response_source] = json_response(conn, 200)
    assert response_source["token"] == Atom.to_string(source.token)
    assert Map.has_key?(response_source, "metrics")
  end

  test "partner credentials without an impersonation target are unauthorized", %{conn: conn} do
    partner = insert(:partner, id: 2_000_000_000)

    conn
    |> add_partner_access_token(partner)
    |> get("/api/sources")
    |> response(401)
  end

  test "ingest, legacy, empty, and public credentials receive ordered minimal sources", %{
    conn: conn,
    user: user,
    alpha: alpha,
    beta: beta
  } do
    expected = [
      %{"token" => Atom.to_string(alpha.token), "name" => alpha.name},
      %{"token" => Atom.to_string(beta.token), "name" => beta.name}
    ]

    for scopes <- ["ingest", "", "public"] do
      assert ^expected =
               conn
               |> add_access_token(user, scopes)
               |> get("/api/sources")
               |> json_response(200)
    end

    assert ^expected =
             conn
             |> put_req_header("x-api-key", user.api_key)
             |> get("/api/sources")
             |> json_response(200)
  end

  test "minimal discovery excludes system sources from JSON and CSV", %{
    conn: conn,
    user: user
  } do
    source = insert(:source, user: user, name: "system.logs", system_source: true)
    source_token = Atom.to_string(source.token)

    for scopes <- ["ingest", "ingest:source:#{source.id}"] do
      json_sources =
        conn
        |> add_access_token(user, scopes)
        |> get("/api/sources")
        |> json_response(200)

      refute Enum.any?(json_sources, &(&1["token"] == source_token))

      csv_sources =
        conn
        |> put_req_header("accept", "text/csv")
        |> add_access_token(user, scopes)
        |> get("/api/sources")
        |> response(200)

      refute csv_sources =~ source_token
    end
  end

  test "source and collection scopes combine and exclude foreign sources", %{
    conn: conn,
    user: user,
    alpha: alpha,
    beta: beta,
    foreign: foreign
  } do
    scopes =
      "ingest:source:#{beta.id} ingest:collection:#{alpha.id} ingest:source:#{foreign.id}"

    assert [
             %{"token" => alpha_token, "name" => "Alpha"},
             %{"token" => beta_token, "name" => "Beta"}
           ] =
             conn
             |> add_access_token(user, scopes)
             |> get("/api/sources")
             |> json_response(200)

    assert alpha_token == Atom.to_string(alpha.token)
    assert beta_token == Atom.to_string(beta.token)

    expected_csv = "token,name\r\n#{alpha_token},Alpha\r\n#{beta_token},Beta\r\n"

    assert ^expected_csv =
             conn
             |> put_req_header("accept", "text/csv")
             |> add_access_token(user, scopes)
             |> get("/api/sources")
             |> response(200)

    foreign_scope = "ingest:source:#{foreign.id}"

    assert [] =
             conn
             |> add_access_token(user, foreign_scope)
             |> get("/api/sources")
             |> json_response(200)

    assert "token,name\r\n" =
             conn
             |> put_req_header("accept", "text/csv")
             |> add_access_token(user, foreign_scope)
             |> get("/api/sources")
             |> response(200)
  end

  test "missing, invalid, and query-only credentials are unauthorized over supported transports",
       %{
         conn: conn,
         user: user
       } do
    conn |> get("/api/sources") |> response(401)

    {:ok, query_token} = Auth.create_access_token(user, %{scopes: "query"})

    for conn <- [
          put_req_header(conn, "authorization", "Bearer #{query_token.token}"),
          put_req_header(conn, "x-api-key", query_token.token),
          conn
        ],
        path <- ["/api/sources", "/api/sources?api_key=#{query_token.token}"] do
      conn |> get(path) |> response(401)
    end

    for conn <- [
          put_req_header(conn, "authorization", "Bearer invalid"),
          put_req_header(conn, "x-api-key", "invalid"),
          conn
        ],
        path <- ["/api/sources", "/api/sources?api_key=invalid"] do
      conn |> get(path) |> response(401)
    end

    malformed = insert(:access_token, resource_owner: user, scopes: "ingest:source:not-an-id")

    conn
    |> put_req_header("authorization", "Bearer #{malformed.token}")
    |> get("/api/sources")
    |> response(401)

    conn
    |> add_access_token(user, "ingest:source:#{String.duplicate("9", 40)}")
    |> get("/api/sources")
    |> response(401)
  end

  test "CSV is RFC 4180, has no management fields, and is never cached", %{conn: conn, user: user} do
    source = insert(:source, user: user, name: "A, \"quoted\"\nsource")

    conn =
      conn
      |> put_req_header("accept", "text/csv")
      |> add_access_token(user, "private")
      |> get("/api/sources")

    assert response(conn, 200) =~ "token,name\r\n"
    assert response(conn, 200) =~ "\"A, \"\"quoted\"\"\nsource\""
    assert get_resp_header(conn, "content-type") == ["text/csv; charset=utf-8"]
    assert get_resp_header(conn, "cache-control") == ["no-store"]
    refute response(conn, 200) =~ "metrics"
    assert response(conn, 200) =~ Atom.to_string(source.token)

    assert "token,name\r\nfoo,bar\r\n" == SourceCsv.encode([%{token: "foo", name: "bar"}])
  end

  test "CSV for an authorized user with no sources contains only the header", %{conn: conn} do
    user = insert(:user)

    conn =
      conn
      |> put_req_header("accept", "text/csv")
      |> add_access_token(user, "ingest")
      |> get("/api/sources")

    assert response(conn, 200) == "token,name\r\n"
    assert get_resp_header(conn, "content-type") == ["text/csv; charset=utf-8"]
  end

  test "unsupported media types are rejected and mutations remain private", %{
    conn: conn,
    user: user
  } do
    assert_error_sent(406, fn ->
      conn
      |> put_req_header("accept", "application/xml")
      |> add_access_token(user, "ingest")
      |> get("/api/sources")
    end)

    conn
    |> add_access_token(user, "ingest")
    |> post("/api/sources", %{name: "not allowed"})
    |> response(401)
  end
end
