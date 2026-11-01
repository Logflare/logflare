defmodule Logflare.Endpoints.Cache do
  @moduledoc """
  Cachex for Endpoints context.
  """

  use Logflare.ContextCache

  alias Logflare.Cache.CachexOps
  alias Logflare.Endpoints
  alias Logflare.Endpoints.EndpointQuery
  alias Logflare.Repo

  @type policy_error :: :database_unavailable | :policy_unavailable

  def child_spec(_) do
    CachexOps.child_spec(__MODULE__,
      limit: 100_000,
      ttl: to_timeout(minute: 2),
      purge_interval: to_timeout(minute: 1),
      transactions: true
    )
  end

  @doc """
  Deduplicates primary reads per endpoint without holding the transaction queue.
  Fills publish only in their captured generation; callers re-read the cache so
  invalidation also supersedes delayed worker replies.
  """
  @spec get_endpoint_policy(integer()) :: EndpointQuery.t() | nil | {:error, policy_error()}
  def get_endpoint_policy(id), do: get_endpoint_policy(id, 3)

  @spec get_endpoint_policy(integer(), non_neg_integer()) ::
          EndpointQuery.t() | nil | {:error, policy_error()}
  defp get_endpoint_policy(id, attempts) do
    {key, _generation_key} = endpoint_policy_keys(id)

    case Cachex.get(__MODULE__, key) do
      {:ok, {:cached, endpoint}} ->
        endpoint

      {:ok, nil} when attempts == 0 ->
        {:error, :policy_unavailable}

      {:ok, nil} ->
        loader = fn -> {:ignore, fetch_endpoint_policy(id)} end

        case Cachex.fetch!(__MODULE__, key, loader) do
          {:error, _reason} = error -> error
          {:cached, _endpoint} -> get_endpoint_policy(id, attempts - 1)
          result when result in [:loaded, :retry] -> get_endpoint_policy(id, attempts - 1)
        end
    end
  end

  @spec fetch_endpoint_policy(integer()) :: :loaded | :retry | {:error, policy_error()}
  defp fetch_endpoint_policy(id) do
    with {:load, generation} <- endpoint_policy_generation(id),
         {:ok, endpoint} <- read_endpoint_policy(id) do
      publish_endpoint_policy(id, generation, endpoint)
    end
  end

  @spec endpoint_policy_generation(integer()) :: :loaded | {:load, reference()}
  defp endpoint_policy_generation(id) do
    {key, generation_key} = endpoint_policy_keys(id)

    Cachex.transaction!(__MODULE__, [key, generation_key], fn cache ->
      case Cachex.get!(cache, key) do
        {:cached, _endpoint} ->
          :loaded

        nil ->
          generation = Cachex.get!(cache, generation_key) || make_ref()
          Cachex.put!(cache, generation_key, generation)
          {:load, generation}
      end
    end)
  end

  @spec publish_endpoint_policy(integer(), reference(), EndpointQuery.t() | nil) ::
          :loaded | :retry
  defp publish_endpoint_policy(id, generation, endpoint) do
    {key, generation_key} = endpoint_policy_keys(id)

    Cachex.transaction!(__MODULE__, [key, generation_key], fn cache ->
      if Cachex.get!(cache, generation_key) == generation do
        Cachex.put!(cache, key, {:cached, endpoint})
        :loaded
      else
        :retry
      end
    end)
  end

  @spec endpoint_policy_keys(integer()) :: {tuple(), tuple()}
  defp endpoint_policy_keys(id),
    do: {{:get_endpoint_policy, [id]}, {:endpoint_policy_generation, [id]}}

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
    {key, generation_key} = endpoint_policy_keys(id)

    result =
      Cachex.transaction(__MODULE__, [key, generation_key], fn cache ->
        Cachex.put!(cache, generation_key, make_ref())
        Cachex.del(cache, key)
      end)

    case result do
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
