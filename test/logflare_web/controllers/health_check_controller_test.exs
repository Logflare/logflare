defmodule LogflareWeb.HealthCheckControllerTest do
  @moduledoc """
  For node-level health check only.
  """
  use LogflareWeb.ConnCase
  alias Logflare.Source.BigQuery.Schema
  alias Logflare.SingleTenant
  alias Logflare.Source

  test "normal node health check", %{conn: conn} do
      start_supervised!(Source.Supervisor)
      :timer.sleep(500)
      conn =
      conn
      |> get("/health")

    assert %{"nodes" => [_], "nodes_count" => 1, "status" => "ok"} = json_response(conn, 200)
  end

  test "coming_up while ets tables not up yet", %{conn: conn} do
    start_supervised!(Source.Supervisor)

    conn =
      conn
      |> get("/health")
    assert %{"status" => "coming_up"} = json_response(conn, 503)

  end

  test "coming_up while RLS boot warming" , %{conn: conn} do
    user = insert(:user)
    insert(:plan)
    for _ <- 1..25 do
      insert(:source, user: user, log_events_updated_at: NaiveDateTime.utc_now())
    end
    start_supervised!(Source.Supervisor)

    conn =
      conn
      |> get("/health")

    assert %{"status" => "coming_up"} = json_response(conn, 503)
      :timer.sleep(1200)

    conn =
      conn
      |> recycle()
      |> get("/health")

      assert %{"status" => "ok"} = json_response(conn, 200)
  end

  describe "Supabase mode - without seed" do
    TestUtils.setup_single_tenant(seed_user: false, supabase_mode: true)

    setup do

      start_supervised!(Source.Supervisor)

:ok
    end
    test "not ok", %{conn: conn} do
      assert %{"status" => "coming_up"} = conn |> get("/health") |> json_response(503)
    end
  end

  describe "Supabase mode - with seed" do
    TestUtils.setup_single_tenant(seed_user: true, supabase_mode: true)

    setup do
    start_supervised!(Source.Supervisor)
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
