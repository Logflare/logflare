defmodule Logflare.DbOutageTest do
  use LogflareWeb.ConnCase, async: false

  alias Logflare.Cache.CachexOps
  alias Logflare.Readiness
  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.Backends.SourceSup
  alias Logflare.Repo
  alias Logflare.Sources.Source
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.SystemMetrics.AllLogsLogged.Poller

  defp unreachable_config(overrides \\ []) do
    Repo.config()
    |> Keyword.merge(
      hostname: "127.0.0.1",
      port: 1,
      pool: DBConnection.ConnectionPool,
      pool_size: 1,
      queue_target: 10,
      queue_interval: 10
    )
    |> Keyword.merge(overrides)
  end

  defp start_unreachable_repo!(_context) do
    {:ok, pid} = Repo.start_link(unreachable_config(name: nil))

    [unreachable_repo: pid]
  end

  defp reset_readiness do
    Readiness.initialize()
    Readiness.mark_ready()
  end

  defp simulate_outage(pid) do
    previous = Repo.get_dynamic_repo()
    Repo.put_dynamic_repo(pid)
    on_exit(fn -> Repo.put_dynamic_repo(previous) end)
  end

  # `simulate_outage/1` only covers direct `Repo` calls: `ContextCache` getters go
  # through `Repo.apply_with_replica/3`, which sets the dynamic repo itself. Both
  # are needed to make every database call in a request fail.
  defp cut_database_connections(pid) do
    simulate_outage(pid)

    entries = [{"unreachable", unreachable_config()}]

    start_supervised!({Repo.Replicas, entries: entries})

    previous = Application.get_env(:logflare, :read_replicas)
    Application.put_env(:logflare, :read_replicas, entries)
    on_exit(fn -> Application.put_env(:logflare, :read_replicas, previous) end)
  end

  defp expect_webhook_success(pid \\ self(), ref \\ make_ref()) do
    WebhookAdaptor.Client
    |> expect(:send, fn _req ->
      send(pid, ref)
      %Tesla.Env{status: 200, body: ""}
    end)

    {pid, ref}
  end

  describe "Repo.get_uptime/0 when the database is unreachable" do
    setup :start_unreachable_repo!

    test "reports zero uptime instead of raising", %{unreachable_repo: pid} do
      simulate_outage(pid)

      assert Repo.get_uptime() == 0
    end
  end

  describe "liveness probe when the database is unreachable" do
    setup :start_unreachable_repo!

    setup do
      start_supervised!(Source.Supervisor)

      Logflare.Google.BigQuery
      |> stub(:init_table!, fn _, _, _, _, _, _ -> :ok end)

      # a warm node: ready, and the startup checks have passed since boot
      reset_readiness()
      Readiness.mark_startup_checks_passed()
      on_exit(&reset_readiness/0)

      :ok
    end

    test "/health stays 200 so the kubelet does not restart the container",
         %{conn: conn, unreachable_repo: pid} do
      simulate_outage(pid)

      assert %{"status" => "ok"} = conn |> get("/health") |> json_response(200)
    end

    test "/ready stays 200 so the pod is not pulled from the Service",
         %{conn: conn, unreachable_repo: pid} do
      simulate_outage(pid)

      assert Readiness.ready?()
      assert %{"status" => "ok"} = conn |> get("/ready") |> json_response(200)
    end

    test "/health answers well inside a probe timeout", %{conn: conn, unreachable_repo: pid} do
      simulate_outage(pid)

      {elapsed_us, conn} = :timer.tc(fn -> get(conn, "/health") end)

      assert %{"status" => "ok"} = json_response(conn, 200)

      assert elapsed_us < 500_000,
             "liveness blocked for #{div(elapsed_us, 1000)}ms; probes default to a 1s timeout"
    end
  end

  describe "liveness probe on a node that has never reached the database" do
    setup :start_unreachable_repo!

    setup do
      start_supervised!(Source.Supervisor)

      Logflare.Google.BigQuery
      |> stub(:init_table!, fn _, _, _, _, _, _ -> :ok end)

      # a cold node: ready to answer probes, but the startup checks have not passed
      reset_readiness()
      on_exit(&reset_readiness/0)

      :ok
    end

    test "/health fails so a cold node is not sent traffic",
         %{conn: conn, unreachable_repo: pid} do
      simulate_outage(pid)

      assert %{"status" => "coming_up"} = conn |> get("/health") |> json_response(503)
    end

    test "/health latches once the primary answers and stops querying it",
         %{conn: conn, unreachable_repo: pid} do
      assert %{"status" => "ok"} = conn |> get("/health") |> json_response(200)
      assert Readiness.startup_checks_passed?()

      simulate_outage(pid)

      assert %{"status" => "ok"} = conn |> get("/health") |> json_response(200)
    end
  end

  describe "ingest auth when credentials cannot be verified" do
    # `ContextCache` getters go through `Repo.apply_with_replica/3`, which sets the
    # dynamic repo itself, so `simulate_outage/1` cannot reach them. Pointing the
    # configured replicas at a closed port makes every context cache miss fail the
    # way it does when the primary is unreachable.
    setup do
      entries = [{"unreachable", unreachable_config()}]

      start_supervised!({Repo.Replicas, entries: entries})

      previous = Application.get_env(:logflare, :read_replicas)
      on_exit(fn -> Application.put_env(:logflare, :read_replicas, previous) end)

      [replica_entries: entries]
    end

    test "legacy api key answers 503 with retry-after rather than a permanent 401",
         %{conn: conn, replica_entries: entries} do
      user = insert(:user)
      insert(:plan)
      source = insert(:source, user: user)

      for cache <- [Logflare.Auth.Cache, Logflare.Users.Cache], do: Cachex.clear(cache)
      Application.put_env(:logflare, :read_replicas, entries)

      conn =
        conn
        |> put_req_header("x-api-key", user.api_key)
        |> post("/logs?source=#{source.token}", %{"message" => "during outage"})

      assert conn.status == 503
      assert get_resp_header(conn, "retry-after") == ["5"]
    end

    test "access token verified from cache but uncached user also answers 503",
         %{conn: conn, replica_entries: entries} do
      user = insert(:user)
      insert(:plan)
      source = insert(:source, user: user)
      {:ok, access_token} = Logflare.Auth.create_access_token(user)

      # warm the token lookup, then drop only the user so the `with` fails on
      # `Users.Cache.get/1` rather than on token verification
      assert {:ok, _token, _user} = Logflare.Auth.Cache.verify_access_token(access_token, [])
      Cachex.clear(Logflare.Users.Cache)
      Application.put_env(:logflare, :read_replicas, entries)

      conn =
        conn
        |> put_req_header("authorization", "Bearer #{access_token.token}")
        |> post("/logs?source=#{source.token}", %{"message" => "during outage"})

      assert conn.status == 503
      assert get_resp_header(conn, "retry-after") == ["5"]
    end
  end

  describe "AllLogsLogged when the database is unreachable" do
    setup :start_unreachable_repo!

    # The callbacks are driven directly because `put_dynamic_repo/1` only affects
    # the calling process, so an outage cannot be simulated for a GenServer running
    # in its own process.
    test "init/1 creates the counter table so ingest can still increment",
         %{unreachable_repo: pid} do
      simulate_outage(pid)

      assert {:ok, state, {:continue, :seed_log_count}} = AllLogsLogged.init([])
      assert {:noreply, ^state} = AllLogsLogged.handle_continue(:seed_log_count, state)

      assert {:ok, _} = AllLogsLogged.increment(:total_logs_logged)
      assert {:ok, count} = AllLogsLogged.log_count(:total_logs_logged)
      assert count >= 1
    end

    test "a persist tick does not raise, so the table is not lost",
         %{unreachable_repo: pid} do
      simulate_outage(pid)

      {:ok, state, _continue} = AllLogsLogged.init([])

      assert {:noreply, ^state} = AllLogsLogged.handle_info(:persist, state)
      assert {:ok, _} = AllLogsLogged.increment(:total_logs_logged)
    end

    test "a persisted count is not overwritten by the events seen during the outage",
         %{unreachable_repo: pid} do
      node = Atom.to_string(node())
      Repo.insert!(%Logflare.SystemMetric{node: node, all_logs_logged: 1_000})

      healthy = Repo.get_dynamic_repo()
      Repo.put_dynamic_repo(pid)
      on_exit(fn -> Repo.put_dynamic_repo(healthy) end)

      {:ok, state, _continue} = AllLogsLogged.init([])
      {:noreply, state} = AllLogsLogged.handle_continue(:seed_log_count, state)
      refute state.seeded?

      for _ <- 1..10, do: AllLogsLogged.increment(:total_logs_logged)

      Repo.put_dynamic_repo(healthy)
      {:noreply, state} = AllLogsLogged.handle_info(:persist, state)

      assert state.seeded?
      assert Repo.get_by(Logflare.SystemMetric, node: node).all_logs_logged == 1_010
    end

    test "a late seed is not reported as logs ingested in that second",
         %{unreachable_repo: pid} do
      node = Atom.to_string(node())
      Repo.insert!(%Logflare.SystemMetric{node: node, all_logs_logged: 1_000})

      healthy = Repo.get_dynamic_repo()
      Repo.put_dynamic_repo(pid)
      on_exit(fn -> Repo.put_dynamic_repo(healthy) end)

      # boot both while the database is unreachable, so the counter is unseeded
      {:ok, counter_state, _continue} = AllLogsLogged.init([])
      {:noreply, counter_state} = AllLogsLogged.handle_continue(:seed_log_count, counter_state)
      {:ok, poller_state} = Poller.init([])

      for _ <- 1..3, do: AllLogsLogged.increment(:total_logs_logged)

      # the seed lands on a later tick, adding 1_000 to the persisted total
      Repo.put_dynamic_repo(healthy)
      {:noreply, _} = AllLogsLogged.handle_info(:persist, counter_state)

      {:noreply, poller_state} = Poller.handle_info(:poll_per_second, poller_state)

      assert poller_state.last_second == 3
    end

    test "a query the database refuses is not rescued" do
      {:ok, refusing} =
        unreachable_config(
          name: nil,
          hostname: Keyword.fetch!(Repo.config(), :hostname),
          port: Keyword.fetch!(Repo.config(), :port),
          after_connect: {Postgrex, :query!, ["set search_path=does_not_exist", []]}
        )
        |> Repo.start_link()

      simulate_outage(refusing)

      {:ok, state, _continue} = AllLogsLogged.init([])

      assert_raise Postgrex.Error, ~r/undefined_table/, fn ->
        AllLogsLogged.handle_continue(:seed_log_count, state)
      end
    end
  end

  describe "full ingestion flow while the database is unreachable" do
    setup :start_unreachable_repo!

    setup do
      start_supervised!(AllLogsLogged)

      insert(:plan, name: "Free")
      user = insert(:user)
      source = insert(:source, user: user)
      insert(:backend, sources: [source], type: :webhook, config: %{url: "some url"})

      start_supervised!({SourceSup, source})

      [user: user, source: source]
    end

    test "a warm node accepts and delivers an event with no database available",
         %{conn: conn, user: user, source: source, unreachable_repo: pid} do
      # warm every ingest-path cache the way real traffic would
      {_pid, warmup_ref} = expect_webhook_success()
      conn = put_req_header(conn, "x-api-key", user.api_key)

      assert conn
             |> post("/logs?source=#{source.token}", %{"message" => "before outage"})
             |> response(200)

      assert_receive ^warmup_ref, 2_000

      cut_database_connections(pid)

      {_pid, ref} = expect_webhook_success()

      assert conn
             |> post("/logs?source=#{source.token}", %{"message" => "during outage"})
             |> response(200)

      assert_receive ^ref, 2_000
    end

    test "a rule routing to a sink source does not crash when the sink is uncached",
         %{conn: conn, user: user, source: source, unreachable_repo: pid} do
      sink = insert(:source, user: user)
      insert(:rule, source: source, sink: sink.token, lql_string: "testing")
      start_supervised!({SourceSup, sink})

      {_pid, warmup_ref} = expect_webhook_success()
      conn = put_req_header(conn, "x-api-key", user.api_key)

      assert conn
             |> post("/logs?source=#{source.token}", %{"message" => "before outage"})
             |> response(200)

      assert_receive ^warmup_ref, 2_000

      # the warmup message does not match the rule, so the sink was never cached
      cut_database_connections(pid)

      {_pid, ref} = expect_webhook_success()

      assert conn
             |> post("/logs?source=#{source.token}", %{"message" => "testing"})
             |> response(200)

      assert_receive ^ref, 2_000
    end
  end

  describe "an uncached plan when the database is unreachable" do
    test "ingest answers 503 rather than raising in the rate limiter", %{conn: conn} do
      user = insert(:user)
      source = insert(:source, user: user)

      Logflare.Billing.Cache
      |> stub(:get_plan_by_user, fn _user -> {:error, :database_unavailable} end)

      conn =
        conn
        |> put_req_header("x-api-key", user.api_key)
        |> post("/logs?source=#{source.token}", %{"message" => "during outage"})

      assert conn.status == 503
      assert get_resp_header(conn, "retry-after") == ["5"]
    end
  end

  describe "nested cache calls when the database is unreachable" do
    test "Auth propagates an unavailable owner lookup instead of wrapping it in :ok" do
      user = insert(:user)
      {:ok, access_token} = Logflare.Auth.create_access_token(user)

      Logflare.Users.Cache
      |> stub(:get, fn _id -> {:error, :database_unavailable} end)

      assert Logflare.Auth.verify_access_token(access_token.token, []) ==
               {:error, :database_unavailable}
    end

    test "a getter returning the error is not committed", %{} do
      cache = Logflare.Users.Cache
      key = {:get, [System.unique_integer([:positive])]}

      assert CachexOps.fetch(cache, key, fn -> {:error, :database_unavailable} end) ==
               {:error, :database_unavailable}

      assert CachexOps.fetch(cache, key, fn -> :recovered end) == :recovered
    end
  end

  describe "CachexOps.fetch/3 when the getter cannot reach the database" do
    setup do
      [cache: Logflare.Users.Cache, key: {:get, [System.unique_integer([:positive])]}]
    end

    test "does not crash the caller", %{cache: cache, key: key} do
      assert CachexOps.fetch(cache, key, fn ->
               raise DBConnection.ConnectionError, "connection not available"
             end) == {:error, :database_unavailable}
    end

    test "lets unrelated exceptions propagate instead of reporting them as an outage",
         %{cache: cache, key: key} do
      assert_raise Cachex.Error, ~r/no function clause matching/, fn ->
        CachexOps.fetch(cache, key, fn -> Integer.parse(:not_a_binary) end)
      end
    end

    test "treats a dead connection pool as unavailable", %{cache: cache, key: key} do
      assert CachexOps.fetch(cache, key, fn ->
               exit({:noproc, {DBConnection, :checkout, []}})
             end) ==
               {:error, :database_unavailable}
    end

    test "raises when the cache is not running", %{key: key} do
      assert_raise RuntimeError, ~r/is not running/, fn ->
        CachexOps.fetch(:no_such_cache, key, fn -> :value end)
      end
    end

    test "does not log call arguments, which carry raw credentials", %{cache: cache} do
      log =
        ExUnit.CaptureLog.capture_log(fn ->
          CachexOps.fetch(cache, {:get_by, [[api_key: "super-secret-api-key"]]}, fn ->
            raise DBConnection.ConnectionError, "connection not available"
          end)
        end)

      refute log =~ "super-secret-api-key"
      assert log =~ "get_by/1"
    end

    test "does not poison the cache once the database recovers", %{cache: cache, key: key} do
      CachexOps.fetch(cache, key, fn ->
        raise DBConnection.ConnectionError, "connection not available"
      end)

      assert CachexOps.fetch(cache, key, fn -> :recovered end) == :recovered
    end
  end
end
