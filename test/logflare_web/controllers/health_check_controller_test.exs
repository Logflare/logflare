defmodule LogflareWeb.HealthCheckControllerTest do
  @moduledoc """
  For node-level health check only.
  """
  use LogflareWeb.ConnCase

  alias Logflare.Backends.Spool.Health
  alias Logflare.Readiness
  alias Logflare.Repo
  alias Logflare.SingleTenant
  alias Logflare.Sources.Source

  setup do
    reset_readiness()
    on_exit(&reset_readiness/0)

    on_exit(fn ->
      Health.report_recovery!(:disk)
      Health.report_recovery!(:upload)
      Health.report_recovery!(:queue)
    end)

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
           } = response = json_response(conn, 200)

    assert Decimal.compare(Decimal.new(response["repo_uptime"]), Decimal.new(0)) == :gt
  end

  test "a dropped Repo checkout makes the health check unhealthy", %{conn: conn} do
    start_supervised!(Source.Supervisor)
    repo = start_uptime_repo(queue_target: 1, queue_interval: 10)
    event = [:db_connection, :connection_error]
    ref = :telemetry_test.attach_event_handlers(self(), [event])
    on_exit(fn -> :telemetry.detach(ref) end)

    with_held_connection(repo, fn ->
      assert %{"status" => "coming_up", "repo_uptime" => 0} =
               conn |> get("/health") |> json_response(503)

      assert_receive {^event, ^ref, %{count: 1},
                      %{error: %DBConnection.ConnectionError{reason: :queue_timeout}}}
    end)
  end

  test "a stalled Repo checkout is bounded independently of queue shedding", %{conn: conn} do
    start_supervised!(Source.Supervisor)
    repo = start_uptime_repo(queue_target: 500, queue_interval: 2_000)

    with_held_connection(repo, fn ->
      started_at = System.monotonic_time(:millisecond)
      response = conn |> get("/health") |> json_response(503)
      elapsed = System.monotonic_time(:millisecond) - started_at

      assert %{"status" => "coming_up", "repo_uptime" => 0} = response
      assert elapsed >= 900
      assert elapsed < 2_000
    end)
  end

  describe "startup latch" do
    test "a full pass latches /health to node-local checks", %{conn: conn} do
      start_supervised!(Source.Supervisor)
      refute Readiness.startup_checks_passed?()

      assert %{"status" => "ok", "checks" => "full"} =
               conn |> get("/health") |> json_response(200)

      assert Readiness.startup_checks_passed?()

      assert %{"status" => "ok", "checks" => "liveness", "repo_uptime" => nil} =
               conn |> get("/health") |> json_response(200)
    end

    test "a failed full check does not latch", %{conn: conn} do
      start_supervised!(Source.Supervisor)
      repo = start_uptime_repo(queue_target: 1, queue_interval: 10)

      with_held_connection(repo, fn ->
        assert %{"status" => "coming_up", "checks" => "full"} =
                 conn |> get("/health") |> json_response(503)
      end)

      refute Readiness.startup_checks_passed?()
    end

    test "a latched node stays healthy while the Repo is unavailable", %{conn: conn} do
      start_supervised!(Source.Supervisor)
      Readiness.mark_startup_checks_passed()
      repo = start_uptime_repo(queue_target: 500, queue_interval: 2_000)

      with_held_connection(repo, fn ->
        started_at = System.monotonic_time(:millisecond)

        assert %{"status" => "ok", "checks" => "liveness", "repo_uptime" => nil} =
                 conn |> get("/health") |> json_response(200)

        assert System.monotonic_time(:millisecond) - started_at < 500
      end)
    end

    test "/ready stops checking the Repo after the latch", %{conn: conn} do
      start_supervised!(Source.Supervisor)
      Readiness.mark_startup_checks_passed()
      repo = start_uptime_repo(queue_target: 500, queue_interval: 2_000)

      with_held_connection(repo, fn ->
        assert %{"status" => "ok", "checks" => "liveness", "repo_uptime" => nil} =
                 conn |> get("/ready") |> json_response(200)
      end)
    end

    test "/ready still checks the Repo before the latch", %{conn: conn} do
      start_supervised!(Source.Supervisor)
      refute Readiness.startup_checks_passed?()
      repo = start_uptime_repo(queue_target: 1, queue_interval: 10)

      with_held_connection(repo, fn ->
        assert %{"status" => "coming_up", "checks" => "full", "repo_uptime" => 0} =
                 conn |> get("/ready") |> json_response(503)
      end)
    end
  end

  test "cache reporting unhealthy", %{conn: conn} do
    start_supervised!(Source.Supervisor)
    stub(Logflare.Partners.Cache, :healthy?, fn -> false end)

    conn = get(conn, "/health")

    assert %{
             "status" => "coming_up",
             "caches" => %{
               "Elixir.Logflare.Partners.Cache" => "unhealthy",
               "Elixir.Logflare.Auth.Cache" => "ok"
             }
           } = json_response(conn, 503)
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

  describe "spool queue reachability" do
    setup do
      insert(:user)
      insert(:plan)
      start_supervised!(Source.Supervisor)

      prev_spool_config = Application.get_env(:logflare, :spool)

      on_exit(fn ->
        if prev_spool_config do
          Application.put_env(:logflare, :spool, prev_spool_config)
        else
          Application.delete_env(:logflare, :spool)
        end
      end)

      :ok
    end

    for mode <- [:producer, :consumer] do
      test "in #{inspect(mode)} mode, an unreachable queue fails /ready but not /health", %{
        conn: conn
      } do
        Application.put_env(:logflare, :spool, mode: unquote(mode), max_spool_health_failures: 1)

        assert %{"status" => "ok", "spool_write_healthy" => %{"queue" => true}} =
                 conn |> get("/ready") |> json_response(200)

        Health.report_failure!(:queue)

        assert %{"status" => "coming_up", "spool_write_healthy" => %{"queue" => false}} =
                 conn |> get("/ready") |> json_response(503)

        assert %{"status" => "ok", "spool_write_healthy" => %{"queue" => false}} =
                 conn |> get("/health") |> json_response(200)

        Health.report_recovery!(:queue)

        assert %{"status" => "ok", "spool_write_healthy" => %{"queue" => true}} =
                 conn |> get("/ready") |> json_response(200)
      end
    end

    test "in :both mode, an unreachable queue is reported but does not fail /ready", %{conn: conn} do
      Application.put_env(:logflare, :spool, mode: :both, max_spool_health_failures: 1)

      Health.report_failure!(:queue)

      assert %{"status" => "ok", "spool_write_healthy" => %{"queue" => false}} =
               conn |> get("/ready") |> json_response(200)
    end

    test "outside spool mode, queue health does not gate /ready", %{conn: conn} do
      Application.delete_env(:logflare, :spool)

      Health.mark_unhealthy!(:queue)

      assert %{"status" => "ok"} = conn |> get("/ready") |> json_response(200)
    end
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

  test "outside :producer mode, spool write health is reported but does not gate the node's own health check",
       %{
         conn: conn
       } do
    insert(:user)
    insert(:plan)
    start_supervised!(Source.Supervisor)

    prev_spool_config = Application.get_env(:logflare, :spool)
    Application.put_env(:logflare, :spool, mode: :both, max_spool_health_failures: 1)

    on_exit(fn ->
      if prev_spool_config do
        Application.put_env(:logflare, :spool, prev_spool_config)
      else
        Application.delete_env(:logflare, :spool)
      end
    end)

    assert %{"status" => "ok", "spool_write_healthy" => %{"disk" => true, "upload" => true}} =
             conn |> get("/health") |> json_response(200)

    Health.report_failure!(:disk)

    # In :both mode, an unhealthy spool falls back to direct backend
    # dispatch (Backends.ingest_logs/4) instead of failing ingest, so this
    # node's own /health check isn't gated on it — see
    # Backends.spool_healthcheck_ok?/0.
    assert %{"status" => "ok", "spool_write_healthy" => %{"disk" => false, "upload" => true}} =
             conn |> get("/health") |> json_response(200)

    Health.report_recovery!(:disk)

    assert %{"status" => "ok", "spool_write_healthy" => %{"disk" => true, "upload" => true}} =
             conn |> get("/health") |> json_response(200)
  end

  test "in :producer mode, an unhealthy disk fails the node's own health check", %{conn: conn} do
    insert(:user)
    insert(:plan)
    start_supervised!(Source.Supervisor)

    prev_spool_config = Application.get_env(:logflare, :spool)
    Application.put_env(:logflare, :spool, mode: :producer, max_spool_health_failures: 1)

    on_exit(fn ->
      if prev_spool_config do
        Application.put_env(:logflare, :spool, prev_spool_config)
      else
        Application.delete_env(:logflare, :spool)
      end
    end)

    assert %{"status" => "ok", "spool_write_healthy" => %{"disk" => true, "upload" => true}} =
             conn |> get("/health") |> json_response(200)

    Health.report_failure!(:disk)

    # :producer-only mode has no backend adaptors to fall back to, so a
    # failing spool must take the node out of rotation instead — see
    # Backends.spool_healthcheck_ok?/0.
    assert %{
             "status" => "coming_up",
             "spool_write_healthy" => %{"disk" => false, "upload" => true}
           } =
             conn |> get("/health") |> json_response(503)

    Health.report_recovery!(:disk)

    assert %{"status" => "ok", "spool_write_healthy" => %{"disk" => true, "upload" => true}} =
             conn |> get("/health") |> json_response(200)
  end

  test "in :producer mode with :wal buffer, an unhealthy upload also fails the node's own health check",
       %{conn: conn} do
    insert(:user)
    insert(:plan)
    start_supervised!(Source.Supervisor)

    prev_spool_config = Application.get_env(:logflare, :spool)

    Application.put_env(:logflare, :spool,
      mode: :producer,
      buffer: :wal,
      max_spool_health_failures: 1
    )

    on_exit(fn ->
      if prev_spool_config do
        Application.put_env(:logflare, :spool, prev_spool_config)
      else
        Application.delete_env(:logflare, :spool)
      end
    end)

    Health.report_failure!(:upload)

    # A WAL that's still absorbing writes locally doesn't mean the spool is
    # actually reaching its durable home — :wal mode writes to both tiers,
    # so both scopes must be healthy, see Backends.spool_healthcheck_ok?/0.
    assert %{
             "status" => "coming_up",
             "spool_write_healthy" => %{"disk" => true, "upload" => false}
           } =
             conn |> get("/health") |> json_response(503)

    Health.report_recovery!(:upload)

    assert %{"status" => "ok", "spool_write_healthy" => %{"disk" => true, "upload" => true}} =
             conn |> get("/health") |> json_response(200)
  end

  test "in :producer mode with :mem buffer, an unhealthy disk does not fail the node's own health check",
       %{conn: conn} do
    insert(:user)
    insert(:plan)
    start_supervised!(Source.Supervisor)

    prev_spool_config = Application.get_env(:logflare, :spool)

    Application.put_env(:logflare, :spool,
      mode: :producer,
      buffer: :mem,
      max_spool_health_failures: 1
    )

    on_exit(fn ->
      if prev_spool_config do
        Application.put_env(:logflare, :spool, prev_spool_config)
      else
        Application.delete_env(:logflare, :spool)
      end
    end)

    Health.report_failure!(:disk)

    # :mem mode has no local disk tier at all, so :disk health doesn't gate
    # it — see Backends.spool_healthcheck_ok?/0.
    assert %{"status" => "ok", "spool_write_healthy" => %{"disk" => false, "upload" => true}} =
             conn |> get("/health") |> json_response(200)

    Health.report_recovery!(:disk)
  end

  describe "Supabase mode - without seed" do
    TestUtils.setup_single_tenant(seed_user: false, supabase_mode: true)

    setup do
      start_supervised!(Source.Supervisor)

      SingleTenant
      |> stub(:supabase_mode_source_schemas_updated?, fn -> true end)

      :ok
    end

    test "incomplete seeding prevents the startup latch", %{conn: conn} do
      assert %{"status" => "coming_up", "checks" => "full"} =
               conn |> get("/health") |> json_response(503)

      refute Readiness.startup_checks_passed?()
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

  defp start_uptime_repo(opts) do
    opts =
      Keyword.merge(
        [name: nil, pool: DBConnection.ConnectionPool, pool_size: 1, timeout: 30_000],
        opts
      )

    start_supervised!({Repo, opts})
  end

  defp with_held_connection(repo, fun) do
    previous_repo = Repo.put_dynamic_repo(repo)
    caller = self()

    holder =
      Task.async(fn ->
        Repo.put_dynamic_repo(repo)

        Repo.checkout(
          fn ->
            send(caller, :connection_held)

            receive do
              :release_connection -> :ok
            end
          end,
          timeout: 10_000
        )
      end)

    try do
      assert_receive :connection_held, 1_000
      fun.()
    after
      Repo.put_dynamic_repo(previous_repo)
      send(holder.pid, :release_connection)
      Task.await(holder, 1_000)
    end
  end

  describe "Supabase mode - with seed" do
    TestUtils.setup_single_tenant(seed_user: true, supabase_mode: true)

    setup do
      start_supervised!(Source.Supervisor)

      SingleTenant
      |> stub(:supabase_mode_source_schemas_updated?, fn -> true end)

      :ok
    end

    test "successful seeding latches health and skips database-dependent seed checks", %{
      conn: conn
    } do
      SingleTenant.create_supabase_sources()
      SingleTenant.create_supabase_endpoints()

      assert %{"status" => "ok", "checks" => "full"} =
               conn |> get("/health") |> json_response(200)

      assert Readiness.startup_checks_passed?()
      stub(SingleTenant, :supabase_mode_status, fn -> raise "unexpected seed status query" end)
      repo = start_uptime_repo(queue_target: 500, queue_interval: 2_000)

      with_held_connection(repo, fn ->
        assert %{"status" => "ok", "checks" => "liveness", "repo_uptime" => nil} =
                 conn |> get("/health") |> json_response(200)
      end)
    end

    # Seeding gates the latch, so a latched node was seeded; re-validating it on
    # every readiness probe would only pull a serving node out during an outage.
    test "/ready stops validating seed status after health has latched", %{conn: conn} do
      SingleTenant.create_supabase_sources()
      SingleTenant.create_supabase_endpoints()

      assert %{"status" => "ok", "checks" => "full"} =
               conn |> get("/health") |> json_response(200)

      assert Readiness.startup_checks_passed?()
      stub(SingleTenant, :supabase_mode_status, fn -> %{seed_user: nil} end)

      assert %{"status" => "ok", "checks" => "liveness"} =
               conn |> get("/ready") |> json_response(200)

      assert Readiness.startup_checks_passed?()
    end
  end
end
