defmodule LogflareWeb.HealthCheckControllerTest do
  @moduledoc """
  For node-level health check only.
  """
  use LogflareWeb.ConnCase
  alias Logflare.Source.BigQuery.Schema
  alias Logflare.SingleTenant

  test "normal node health check", %{conn: conn} do
    conn =
      conn
      |> get("/health")

    assert %{"nodes" => [_], "nodes_count" => 1, "status" => "ok"} = json_response(conn, 200)
  end

  describe "Supabase mode - without seed" do
    TestUtils.setup_single_tenant(seed_user: false, supabase_mode: true)

    test "not ok", %{conn: conn} do
      assert %{"status" => "coming_up"} = conn |> get("/health") |> json_response(503)
    end
  end

  describe "Supabase mode - with seed" do
    TestUtils.setup_single_tenant(seed_user: true, supabase_mode: true)

    setup do
      stub(Schema, :get_state, fn _ -> %{field_count: 5} end)
      :ok
    end

    test "ok", %{conn: conn} do
      SingleTenant.create_supabase_sources()
      SingleTenant.create_supabase_endpoints()
      assert %{"status" => "ok"} = conn |> get("/health") |> json_response(200)
    end
  end
end
