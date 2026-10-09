defmodule Logflare.Endpoints.Cache do
  @moduledoc """
  Cachex for Endpoints context.
  """

  use Logflare.ContextCache

  alias Logflare.Cache.CachexOps
  alias Logflare.Endpoints

  def child_spec(_) do
    CachexOps.child_spec(__MODULE__,
      limit: 100_000,
      ttl: to_timeout(minute: 2),
      purge_interval: to_timeout(minute: 1)
    )
  end

  def get_endpoint_query(kw), do: apply_repo_fun(:get_endpoint_query, [kw])

  def get_endpoint_query_at_version(query_id, version_number),
    do: apply_repo_fun(:get_endpoint_query_at_version, [query_id, version_number])

  def get_by(kw), do: apply_repo_fun(:get_by, [kw])
  def get_mapped_query_by_token(token), do: apply_repo_fun(:get_mapped_query_by_token, [token])

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(Endpoints, arg1, arg2)
  end
end
