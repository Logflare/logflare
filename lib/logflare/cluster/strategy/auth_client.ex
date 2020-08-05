defmodule Logflare.Cluster.Strategy.GoogleComputeEngine.AuthClient do
  require Logger

  use Tesla

  plug Tesla.Middleware.Retry,
    delay: 500,
    max_retries: 10,
    max_delay: 4_000,
    should_retry: fn
      {:ok, %{status: status}} when status in 500..599 -> true
      {:ok, _} -> false
      {:error, _} -> true
    end

  plug Tesla.Middleware.BaseUrl, "http://metadata.google.internal/computeMetadata/v1"
  plug Tesla.Middleware.JSON

  adapter(Tesla.Adapter.Mint, timeout: 60_000, mode: :passive)

  def metadata() do
    get("/instance/service-accounts/default/token", headers: [{"Metadata-Flavor", "Google"}])
  end
end
