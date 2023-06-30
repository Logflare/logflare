defmodule LogflareWeb.HealthCheckControllerTest do
  @moduledoc """
  For node-level health check only.
  """
  use LogflareWeb.ConnCase
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
      %{username: username, password: password, database: database, hostname: hostname} =
        Application.get_env(:logflare, Logflare.Repo) |> Map.new()

      url = "postgresql://#{username}:#{password}@#{hostname}/#{database}"
      previous_url = Application.get_env(:logflare, :single_instance_postgres_url)
      Application.put_env(:logflare, :single_instance_postgres_url, url)

      on_exit(fn ->
        Application.put_env(:logflare, :single_instance_postgres_url, previous_url)
      end)

      %{url: url}
    end

    test "ok", %{conn: conn} do
      SingleTenant.create_supabase_sources()
      SingleTenant.create_supabase_endpoints()
      started = SingleTenant.ensure_supabase_sources_started() |> Enum.map(&elem(&1, 1))
      assert %{"status" => "ok"} = conn |> get("/health") |> json_response(200)

      on_exit(fn ->
        Enum.each(started, &Process.exit(&1, :normal))
      end)
    end
  end
end
