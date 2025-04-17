defmodule Logflare.Cluster.Strategy.GoogleComputeEngine.ComputeClient do
  @moduledoc """
  Inteface to GCP for the cluster strategy.
  """
  require Logger
  use Tesla
  defp env_project_id, do: Application.get_env(:logflare, Logflare.Google)[:project_id]

  plug Tesla.Middleware.Retry,
    delay: 500,
    max_retries: 10,
    max_delay: 4_000,
    should_retry: fn
      {:ok, %{status: status}} when status in 404..599 -> true
      {:ok, _} -> false
      {:error, _} -> true
    end

  plug Tesla.Middleware.BaseUrl,
       "https://compute.googleapis.com/compute/v1/projects/#{env_project_id()}"

  plug Tesla.Middleware.JSON

  adapter(Tesla.Adapter.Mint, timeout: 60_000, mode: :passive)

  def zone_nodes(zone, group_name, auth_token) do
    post("/zones/" <> zone <> "/instanceGroups/" <> group_name <> "/listInstances", "",
      headers: [{"Authorization", "Bearer #{auth_token}"}]
    )
  end

  def region_nodes(region, group_name, auth_token) do
    post("/regions/" <> region <> "/instanceGroups/" <> group_name <> "/listInstances", "",
      headers: [{"Authorization", "Bearer #{auth_token}"}]
    )
  end

  def node_metadata(url, auth_token) do
    get(url, headers: [{"Authorization", "Bearer #{auth_token}"}])
  end
end
