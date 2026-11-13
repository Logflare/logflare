defmodule LogflareWeb.HealthCheckController do
  use LogflareWeb, :controller

  alias Logflare.Backends
  alias Logflare.Backends.Spool.Health, as: SpoolHealth
  alias Logflare.JSON
  alias Logflare.Cluster
  alias Logflare.Readiness
  alias Logflare.SingleTenant
  alias Logflare.Sources
  alias Logflare.System

  @doc """
  Readiness: runs every check until the node has passed once, then only the
  node-local ones, and fails once graceful shutdown begins. Spool nodes also
  need their queue to be reachable.

  Ingest resolves everything it needs from the context caches, so a node that has
  already passed stays in rotation through a database outage rather than being
  pulled out while it is still serving.
  """
  def ready(conn, _params) do
    if Readiness.ready?() do
      respond(conn, startup_mode(), [Backends.spool_queue_ready?()])
    else
      response = JSON.encode!(%{status: :not_ready})

      conn
      |> put_resp_content_type("application/json")
      |> send_resp(503, response)
    end
  end

  @doc """
  Health: runs every check until the node has passed once, then only the
  node-local ones. Load balancers that have a single health check (GCP) then
  hold a booting node out of rotation until it can reach the database, without
  dropping or recreating a running node when the database is slow.
  """
  def check(conn, _params) do
    respond(conn, startup_mode(), [])
  end

  defp startup_mode do
    if Readiness.startup_checks_passed?(), do: :liveness, else: :full
  end

  defp respond(conn, mode, extra_checks) do
    repo_uptime = if mode == :full, do: Logflare.Repo.get_uptime()
    caches = check_caches()
    memory_utilization = System.memory_utilization()
    max_memory_ratio = Application.get_env(:logflare, :health) |> Keyword.get(:memory_utilization)

    common_checks_ok? =
      [
        Sources.ingest_ets_tables_started?(),
        # checks that db can execute query and that repo is connected and up
        mode == :liveness or repo_uptime > 0,
        Enum.all?(Map.values(caches), &(&1 == :ok)),
        memory_utilization < max_memory_ratio,
        Backends.spool_healthcheck_ok?()
        | extra_checks
      ]
      |> Enum.all?()

    {status, code} =
      cond do
        # Supabase mode's seed status reads the database, so it is a startup check.
        mode == :full and SingleTenant.supabase_mode?() and common_checks_ok? ->
          status = SingleTenant.supabase_mode_status()
          values = Map.values(status)

          if Enum.any?(values, &is_nil/1) do
            {:coming_up, 503}
          else
            {:ok, 200}
          end

        common_checks_ok? == false ->
          {:coming_up, 503}

        true ->
          {:ok, 200}
      end

    if mode == :full and code == 200, do: Readiness.mark_startup_checks_passed()

    response =
      status
      |> build_payload(
        checks: mode,
        repo_uptime: repo_uptime,
        caches: caches,
        memory_utilization: if(memory_utilization < max_memory_ratio, do: :ok, else: :critical),
        spool_write_healthy: %{
          disk: SpoolHealth.healthy?(:disk),
          upload: SpoolHealth.healthy?(:upload),
          queue: SpoolHealth.healthy?(:queue)
        }
      )
      |> JSON.encode!()

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(code, response)
  end

  defp build_payload(status,
         checks: checks,
         repo_uptime: repo_uptime,
         caches: caches,
         memory_utilization: memory_utilization,
         spool_write_healthy: spool_write_healthy
       )
       when status in [:ok, :coming_up] do
    nodes = Cluster.Utils.node_list_all()
    proc_count = Process.list() |> Enum.count()

    %{
      status: status,
      checks: checks,
      proc_count: proc_count,
      this_node: Node.self(),
      nodes: nodes,
      nodes_count: Enum.count(nodes),
      spool_write_healthy: spool_write_healthy,
      repo_uptime: repo_uptime,
      caches: caches,
      memory_utilization: memory_utilization
    }
  end

  defp check_caches do
    for cache <-
          Logflare.ContextCache.Supervisor.list_caches() ++
            [
              Logflare.Logs.LogEvents.Cache
            ],
        into: %{} do
      {cache, if(cache.healthy?(), do: :ok, else: :unhealthy)}
    end
  end
end
