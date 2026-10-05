defmodule LogflareWeb.HealthCheckController do
  use LogflareWeb, :controller

  alias Logflare.Backends.Spool.Health, as: SpoolHealth
  alias Logflare.JSON
  alias Logflare.Cluster
  alias Logflare.Readiness
  alias Logflare.SingleTenant
  alias Logflare.Sources
  alias Logflare.System

  @db_answered_key {__MODULE__, :db_answered_once?}

  @doc """
  Readiness probe: whether this node should receive traffic.

  Does not check the primary database - ingest resolves everything it needs from
  the context caches, so a node with an unreachable primary is still serving.
  """
  def ready(conn, params) do
    if Readiness.ready?() do
      check(conn, params)
    else
      response = JSON.encode!(%{status: :not_ready})

      conn
      |> put_resp_content_type("application/json")
      |> send_resp(503, response)
    end
  end

  @doc """
  Liveness probe: whether this BEAM is healthy.

  The primary database is only checked until it has answered once. A node that
  has never reached it has cold caches and no way to warm them, so it must not
  take traffic. Once it has, database availability stops being a liveness
  concern: restarting cannot fix an unreachable database, and it discards the
  caches ingest needs to ride out the outage.
  """
  def check(conn, _params) do
    caches = check_caches()
    memory_utilization = System.memory_utilization()
    max_memory_ratio = Application.get_env(:logflare, :health) |> Keyword.get(:memory_utilization)

    common_checks_ok? =
      [
        Sources.ingest_ets_tables_started?(),
        db_answered_once?(),
        Enum.all?(Map.values(caches), &(&1 == :ok)),
        memory_utilization < max_memory_ratio
        # Temporarily not gating the health check on SpoolHealth.healthy?()
        # until it's been observed in production for a while.
      ]
      |> Enum.all?()

    {status, code} =
      cond do
        SingleTenant.supabase_mode?() and common_checks_ok? ->
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

    response =
      status
      |> build_payload(
        caches: caches,
        memory_utilization: if(memory_utilization < max_memory_ratio, do: :ok, else: :critical),
        spool_write_healthy: %{
          disk: SpoolHealth.healthy?(:disk),
          upload: SpoolHealth.healthy?(:upload)
        }
      )
      |> JSON.encode!()

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(code, response)
  end

  defp build_payload(status,
         caches: caches,
         memory_utilization: memory_utilization,
         spool_write_healthy: spool_write_healthy
       )
       when status in [:ok, :coming_up] do
    nodes = Cluster.Utils.node_list_all()
    proc_count = Process.list() |> Enum.count()

    %{
      status: status,
      proc_count: proc_count,
      this_node: Node.self(),
      nodes: nodes,
      nodes_count: Enum.count(nodes),
      spool_write_healthy: spool_write_healthy,
      caches: caches,
      memory_utilization: memory_utilization
    }
  end

  defp db_answered_once? do
    :persistent_term.get(@db_answered_key, false) or
      with true <- db_reachable?(Logflare.Repo.get_uptime()) do
        :persistent_term.put(@db_answered_key, true)
        true
      end
  end

  defp db_reachable?(%Decimal{} = uptime), do: Decimal.compare(uptime, 0) == :gt
  defp db_reachable?(uptime) when is_number(uptime), do: uptime > 0

  defp check_caches do
    for cache <-
          Logflare.ContextCache.Supervisor.list_caches() ++
            [
              Logflare.Logs.LogEvents.Cache
            ],
        into: %{} do
      # call is O(1)
      case Cachex.size(cache) do
        {:ok, _} -> {cache, :ok}
        {:error, :no_cache} -> {cache, :no_cache}
      end
    end
  end
end
