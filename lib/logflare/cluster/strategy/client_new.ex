defmodule Logflare.Cluster.Strategy.GoogleComputeEngine.Staging.ClientNew do
  require Logger

  @middleware [Tesla.Middleware.JSON]

  @adapter Tesla.Adapter.Hackney

  def new() do
    middleware =
      [
        {Tesla.Middleware.Retry,
         delay: 500,
         max_retries: 10,
         max_delay: 4_000,
         should_retry: fn
           {:ok, %{status: status}} when status in 500..599 -> true
           {:ok, _} -> false
           {:error, _} -> true
         end},
        {Tesla.Middleware.BaseUrl, "http://metadata.google.internal/computeMetadata/v1"}
      ] ++ @middleware

    adapter = {@adapter, pool: __MODULE__, recv_timeout: 60_000}

    Tesla.client(middleware, adapter)
  end

  def zone_nodes(client, zone, group_name, auth_token) do
    Tesla.post(
      client,
      "/zones/" <> zone <> "/instanceGroups/" <> group_name <> "/listInstances",
      "",
      headers: [{"Authorization", auth_token}]
    )
  end

  def region_nodes(client, region, group_name, auth_token) do
    Tesla.post(
      client,
      "/regions/" <> region <> "/instanceGroups/" <> group_name <> "listInstances",
      "",
      headers: [{"Authorization", auth_token}]
    )
  end

  def node_metadata(client, url, auth_token) do
    Tesla.get(client, url, headers: [{"Authorization", auth_token}])
  end

  def metadata(client) do
    Tesla.get(client, "/instance/service-accounts/default/token",
      headers: [{"Metadata-Flavor", "Google"}]
    )
  end
end
