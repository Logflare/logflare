defmodule LogflareWeb.HealthCheckController do
  use LogflareWeb, :controller

  alias Logflare.JSON
  alias Logflare.Cluster
  alias Logflare.SingleTenant
  alias Logflare.Sources
  alias Logflare.System

  def check(conn, _params) do
    repo_uptime = Logflare.Repo.get_uptime()
    caches = check_caches()
    memory_utilization = System.memory_utilization()
    max_memory_ratio = Application.get_env(:logflare, :health) |> Keyword.get(:memory_utilization)

    common_checks_ok? =
      [
        Sources.ingest_ets_tables_started?(),
        # checks that db can execute query and that repo is connected and up
        repo_uptime > 0,
        Enum.all?(Map.values(caches), &(&1 == :ok)),
        memory_utilization < max_memory_ratio
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
        repo_uptime: repo_uptime,
        caches: caches,
        memory_utilization: if(memory_utilization < max_memory_ratio, do: :ok, else: :critical)
      )
      |> JSON.encode!()

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(code, response)
  end

  defp build_payload(status,
         repo_uptime: repo_uptime,
         caches: caches,
         memory_utilization: memory_utilization
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
      # call is O(1)
      case Cachex.size(cache) do
        {:ok, _} -> {cache, :ok}
        {:error, :no_cache} -> {cache, :no_cache}
      end
    end
  end
end
