defmodule Logflare.Endpoints.Cache do
  @moduledoc """
  Cachex for Endpoints context.
  """

  use Logflare.ContextCache

  alias Logflare.Cache.CachexOps
  alias Logflare.Endpoints
  alias Logflare.Endpoints.EndpointQuery
  alias Logflare.Repo

  def child_spec(_) do
    CachexOps.child_spec(__MODULE__,
      limit: 100_000,
      ttl: to_timeout(minute: 2),
      purge_interval: to_timeout(minute: 1),
      transactions: true
    )
  end

  @spec get_endpoint_policy(integer()) ::
          EndpointQuery.t() | nil | {:error, :database_unavailable}
  def get_endpoint_policy(id) do
    key = {:get_endpoint_policy, [id]}

    case Cachex.get(__MODULE__, key) do
      {:ok, {:cached, endpoint}} ->
        endpoint

      {:ok, nil} ->
        {:ok, result} =
          Cachex.transaction(__MODULE__, [key], &fetch_endpoint_policy(&1, key, id))

        result
    end
  end

  @spec fetch_endpoint_policy(Cachex.t(), tuple(), integer()) ::
          EndpointQuery.t() | nil | {:error, :database_unavailable}
  defp fetch_endpoint_policy(cache, key, id) do
    case Cachex.get(cache, key) do
      {:ok, {:cached, endpoint}} ->
        endpoint

      {:ok, nil} ->
        with {:ok, endpoint} <- read_endpoint_policy(id) do
          {:ok, true} = Cachex.put(cache, key, {:cached, endpoint})
          endpoint
        end
    end
  end

  @spec read_endpoint_policy(integer()) ::
          {:ok, EndpointQuery.t() | nil} | {:error, :database_unavailable}
  defp read_endpoint_policy(id) do
    {:ok, Repo.with_primary(fn -> Endpoints.get_endpoint_query(id) end)}
  rescue
    _error in DBConnection.ConnectionError -> {:error, :database_unavailable}
  catch
    :exit, {reason, _call} when reason in [:noproc, :killed] ->
      {:error, :database_unavailable}
  end

  @spec invalidate_endpoint_policy(integer()) :: :ok | {:error, term()}
  def invalidate_endpoint_policy(id) do
    key = {:get_endpoint_policy, [id]}

    case Cachex.transaction(__MODULE__, [key], &Cachex.del(&1, key)) do
      {:ok, {:ok, true}} -> :ok
      error -> {:error, error}
    end
  end

  @doc """
  Invalidates policy entries on connected nodes. Nodes predating this API have no
  entries to evict, but must still be upgraded before enabling endpoint policy.
  """
  @spec invalidate_endpoint_policy_cluster(integer()) ::
          :ok | {:error, :cache_invalidation_failed}
  def invalidate_endpoint_policy_cluster(id) do
    [node() | Node.list()]
    |> :erpc.multicall(__MODULE__, :invalidate_endpoint_policy, [id], 5_000)
    |> Enum.reduce(:ok, fn
      {:ok, :ok}, status ->
        status

      {:error, {:exception, :undef, [{__MODULE__, :invalidate_endpoint_policy, [^id], []}]}},
      status ->
        status

      _failure, _status ->
        {:error, :cache_invalidation_failed}
    end)
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
