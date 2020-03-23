defmodule Logflare.Cluster.Strategy.GoogleComputeEngine.Client do
  require Logger

  use Tesla

  plug Tesla.Middleware.JSON

  plug Tesla.Middleware.Retry,
    delay: 500,
    max_retries: 10,
    max_delay: 4_000,
    should_retry: fn
      {:ok, %{status: status}} when status in 500..599 -> true
      {:ok, _} -> false
      {:error, _} -> true
    end

  plug Tesla.Middleware.Headers, [{"Content-Type", "application/json"}]

  plug Tesla.Middleware.BaseUrl, "http://metadata.google.internal/computeMetadata/v1"

  adapter(Tesla.Adapter.Hackney, pool: __MODULE__, recv_timeout: 60_000)

  def zone_nodes(zone, group_name, auth_token) do
    post("/zones/" <> zone <> "/instanceGroups/" <> group_name <> "/listInstances",
      headers: [{"Authorization", auth_token}]
    )
  end

  def region_nodes(region, group_name, auth_token) do
    post("/regions/" <> region <> "/instanceGroups/" <> group_name <> "listInstances",
      headers: [{"Authorization", auth_token}]
    )
  end

  def node_metadata(url, auth_token) do
    get(url, headers: [{"Authorization", auth_token}])
  end

  def metadata() do
    get("/instance/service-accounts/default/token", headers: [{"Metadata-Flavor", "Google"}])
  end
end
