defmodule Logflare.DbOutageTest do
  use LogflareWeb.ConnCase, async: false

  alias Logflare.ContextCache
  alias Logflare.Readiness
  alias Logflare.Repo
  alias Logflare.Sources.Source

  # A repo pool pointed at a closed port. Queries against it fail the same way
  # they do when the primary database is unreachable, without touching the
  # sandbox-owned connection the rest of the suite depends on.
  defp start_unreachable_repo!(_context) do
    {:ok, pid} =
      Repo.start_link(
        name: nil,
        hostname: "127.0.0.1",
        port: 1,
        pool: DBConnection.ConnectionPool,
        pool_size: 1,
        queue_target: 10,
        queue_interval: 10
      )

    [unreachable_repo: pid]
  end

  defp simulate_outage(pid) do
    previous = Repo.get_dynamic_repo()
    Repo.put_dynamic_repo(pid)
    on_exit(fn -> Repo.put_dynamic_repo(previous) end)
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
      refute Map.has_key?(json_response(conn, 200), "repo_uptime")

      assert elapsed_us < 500_000,
             "liveness blocked for #{div(elapsed_us, 1000)}ms; probes default to a 1s timeout"
    end

    test "/startup still fails, so a cold node is not sent traffic",
         %{conn: conn, unreachable_repo: pid} do
      simulate_outage(pid)

      assert %{"status" => "coming_up"} = conn |> get("/startup") |> json_response(503)
    end
  end

  describe "startup probe with a reachable database" do
    setup do
      start_supervised!(Source.Supervisor)

      Logflare.Google.BigQuery
      |> stub(:init_table!, fn _, _, _, _, _, _ -> :ok end)

      :ok
    end

    test "/startup passes once the primary answers", %{conn: conn} do
      assert %{"status" => "ok"} = conn |> get("/startup") |> json_response(200)
    end
  end

  describe "ingest auth when credentials cannot be verified" do
    # `ContextCache` getters go through `Repo.apply_with_replica/3`, which sets the
    # dynamic repo itself, so `simulate_outage/1` cannot reach them. Pointing the
    # configured replicas at a closed port makes every context cache miss fail the
    # way it does when the primary is unreachable.
    setup do
      entries = [
        {"unreachable",
         [
           hostname: "127.0.0.1",
           port: 1,
           pool: DBConnection.ConnectionPool,
           pool_size: 1,
           queue_target: 10,
           queue_interval: 10
         ]}
      ]

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

  describe "ContextCache.fetch/3 when the getter cannot reach the database" do
    setup do
      [cache: Logflare.Users.Cache, key: {:get, [System.unique_integer([:positive])]}]
    end

    test "does not crash the caller", %{cache: cache, key: key} do
      assert ContextCache.fetch(cache, key, fn ->
               raise DBConnection.ConnectionError, "connection not available"
             end) == {:error, :database_unavailable}
    end

    test "lets unrelated exceptions propagate instead of reporting them as an outage",
         %{cache: cache, key: key} do
      assert_raise FunctionClauseError, fn ->
        ContextCache.fetch(cache, key, fn -> Integer.parse(:not_a_binary) end)
      end
    end

    test "treats a dead connection pool as unavailable", %{cache: cache, key: key} do
      assert ContextCache.fetch(cache, key, fn ->
               exit({:noproc, {DBConnection, :checkout, []}})
             end) ==
               {:error, :database_unavailable}
    end

    test "does not log call arguments, which carry raw credentials", %{cache: cache} do
      log =
        ExUnit.CaptureLog.capture_log(fn ->
          ContextCache.fetch(cache, {:get_by, [[api_key: "super-secret-api-key"]]}, fn ->
            raise DBConnection.ConnectionError, "connection not available"
          end)
        end)

      refute log =~ "super-secret-api-key"
      assert log =~ "get_by/1"
    end

    test "does not poison the cache once the database recovers", %{cache: cache, key: key} do
      ContextCache.fetch(cache, key, fn ->
        raise DBConnection.ConnectionError, "connection not available"
      end)

      assert ContextCache.fetch(cache, key, fn -> :recovered end) == :recovered
    end
  end
end
