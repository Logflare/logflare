defmodule LogflareWeb.ConnCaseTest do
  use ExUnit.Case, async: true

  alias LogflareWeb.ConnCase

  test "put_default_team_param/2 adds the selected team without losing query parameters" do
    conn =
      Phoenix.ConnTest.build_conn()
      |> Plug.Conn.put_private(:logflare_test_team_id, "team-123")

    result = ConnCase.put_default_team_param(conn, "/search?tailing=false")
    uri = URI.parse(result)

    assert uri.path == "/search"
    assert URI.decode_query(uri.query) == %{"t" => "team-123", "tailing" => "false"}
  end

  test "put_default_team_param/2 preserves an explicitly selected team" do
    conn =
      Phoenix.ConnTest.build_conn()
      |> Plug.Conn.put_private(:logflare_test_team_id, "team-123")

    path = "/search?t=team-456&tailing=false"
    assert ConnCase.put_default_team_param(conn, path) == path
  end

  test "prepare_live_with_redirect/3 can bypass the default team parameter" do
    conn =
      Phoenix.ConnTest.build_conn()
      |> Plug.Conn.put_private(:logflare_test_team_id, "team-123")

    path = "/search?tailing=false"

    assert {^path, [on_error: :warn]} =
             ConnCase.prepare_live_with_redirect(conn, path,
               bypass_team_param: true,
               on_error: :warn
             )
  end

  test "redirected_to_different_team?/2 only follows a different selected team" do
    assert ConnCase.redirected_to_different_team?("/search?t=team-123", "/search?t=team-456")
    refute ConnCase.redirected_to_different_team?("/search?t=team-123", "/search?t=team-123")
    refute ConnCase.redirected_to_different_team?("/search?t=team-123", "/search")
  end
end
