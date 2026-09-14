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
  end

  describe "ContextCache.fetch/3 when the getter cannot reach the database" do
    setup do
      [cache: Logflare.Users.Cache, key: {:get, [System.unique_integer([:positive])]}]
    end

    test "does not crash the caller", %{cache: cache, key: key} do
      ContextCache.fetch(cache, key, fn ->
        raise DBConnection.ConnectionError, "connection not available"
      end)
    end

    test "does not poison the cache once the database recovers", %{cache: cache, key: key} do
      ContextCache.fetch(cache, key, fn ->
        raise DBConnection.ConnectionError, "connection not available"
      end)

      assert ContextCache.fetch(cache, key, fn -> :recovered end) == :recovered
    end
  end
end
