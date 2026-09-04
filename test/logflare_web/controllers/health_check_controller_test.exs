defmodule LogflareWeb.HealthCheckControllerTest do
  @moduledoc """
  For node-level health check only.
  """
  use LogflareWeb.ConnCase

  alias Logflare.Backends.Spool.WriteHealth
  alias Logflare.Readiness
  alias Logflare.SingleTenant
  alias Logflare.Sources.Source

  setup do
    reset_readiness()
    on_exit(&reset_readiness/0)
    on_exit(fn -> WriteHealth.report_recovery!() end)

    Logflare.Google.BigQuery
    |> stub(:init_table!, fn _, _, _, _, _, _ -> :ok end)

    :ok
  end

  test "normal node health check", %{conn: conn} do
    start_supervised!(Source.Supervisor)

    conn = get(conn, "/health")

    assert %{
             "nodes" => [_],
             "nodes_count" => 1,
             "status" => "ok",
             "caches" => %{
               "Elixir.Logflare.Auth.Cache" => "ok"
             }
           } = json_response(conn, 200)
  end

  test "readiness check", %{conn: conn} do
    start_supervised!(Source.Supervisor)
    :timer.sleep(1000)

    assert %{"status" => "ok"} = conn |> get("/ready") |> json_response(200)
  end

  test "readiness check while draining", %{conn: conn} do
    start_supervised!(Source.Supervisor)
    :timer.sleep(1000)
    Readiness.begin_draining()

    assert %{"status" => "not_ready"} = conn |> get("/ready") |> json_response(503)
    assert %{"status" => "ok"} = conn |> get("/health") |> json_response(200)
  end

  test "memory check", %{conn: conn} do
    insert(:user)
    insert(:plan)
    start_supervised!(Source.Supervisor)

    conn =
      conn
      |> get("/health")

    assert %{"memory_utilization" => "ok"} = json_response(conn, 200)
  end

  test "spool write health check", %{conn: conn} do
    insert(:user)
    insert(:plan)
    start_supervised!(Source.Supervisor)

    assert %{"spool_write_healthy" => true} = conn |> get("/health") |> json_response(200)

    WriteHealth.report_failure!()

    assert %{"status" => "coming_up", "spool_write_healthy" => false} =
             conn |> get("/health") |> json_response(503)

    WriteHealth.report_recovery!()

    assert %{"status" => "ok", "spool_write_healthy" => true} =
             conn |> get("/health") |> json_response(200)
  end

  describe "Supabase mode - without seed" do
    TestUtils.setup_single_tenant(seed_user: false, supabase_mode: true)

    setup do
      start_supervised!(Source.Supervisor)

      SingleTenant
      |> stub(:supabase_mode_source_schemas_updated?, fn -> true end)

      :ok
    end

    test "not ok", %{conn: conn} do
      assert %{"status" => "coming_up"} = conn |> get("/health") |> json_response(503)
    end

    test "not ready even when the application is accepting traffic", %{conn: conn} do
      Readiness.mark_ready()

      assert %{"status" => "coming_up"} = conn |> get("/ready") |> json_response(503)
    end
  end

  defp reset_readiness do
    Readiness.initialize()
    Readiness.mark_ready()
  end

  describe "Supabase mode - with seed" do
    TestUtils.setup_single_tenant(seed_user: true, supabase_mode: true)

    setup do
      start_supervised!(Source.Supervisor)

      SingleTenant
      |> stub(:supabase_mode_source_schemas_updated?, fn -> true end)

      :ok
    end

    test "ok", %{conn: conn} do
      # :timer.sleep(500)
      SingleTenant.create_supabase_sources()
      SingleTenant.create_supabase_endpoints()

      assert %{"status" => "ok"} = conn |> get("/health") |> json_response(200)
    end
  end
end
