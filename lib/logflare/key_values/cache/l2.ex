defmodule Logflare.KeyValues.Cache.L2 do
  @moduledoc """
  Distributed (L2) level of `Logflare.KeyValues.Cache`, consulted on an L1 miss before the
  database.

  Keys are partitioned across the cluster by a consistent hash ring; each node holds its
  share in a `Nebulex.Adapters.Local` primary storage, reachable through `__primary__/0`
  without RPC. It is off unless `enabled?/0`, and the settings below live under
  `config :logflare, Logflare.KeyValues.Cache`:

    * `:l2_enabled` - starts the level at boot; the cache treats an enabled level as running,
      so it is not meant to change at runtime
    * `:l2_timeout` - RPC timeout in ms; a slower L2 call counts as an error
    * `:l2_ttl` - time-to-live in ms of an entry written to L2

  Adapter options come from `config :logflare, Logflare.KeyValues.Cache.L2`. The defaults:

    * `primary: [compressed: true]` - values are JSON maps, compressed as in L1
    * `primary: [gc_interval: 1h]` - generations rotate hourly, so with the 1h `:l2_ttl`
      expired entries are reclaimed within two hours even if never read again
    * `primary: [max_size: 2_000_000, allocated_memory: 1 GiB]` - caps one node's share;
      crossing either drops the older generation instead of growing the VM
    * `rejoin_interval: 30s` - the adapter default; a missed `:pg` join only routes keys to
      a different owner (a miss), never serves a wrong value, so faster convergence buys little
    * `hash_ring` - `ExHashRing` defaults (512 vnodes per node)
  """

  use Nebulex.Cache,
    otp_app: :logflare,
    adapter: Nebulex.Adapters.Partitioned

  alias Logflare.ContextCache.Tombstones
  alias Logflare.KeyValues
  alias Logflare.Utils.Tasks
  alias Nebulex.Distributed.RPC

  @fill_event [:logflare, :key_values, :cache, :l2_fill]

  @type result() :: {:ok, term()} | :miss | :l2_error | :l2_skipped

  @spec enabled?() :: boolean()
  def enabled?, do: Keyword.get(settings(), :l2_enabled, false)

  @spec timeout() :: timeout()
  def timeout, do: Keyword.get(settings(), :l2_timeout, 50)

  @spec ttl() :: timeout()
  def ttl, do: Keyword.get(settings(), :l2_ttl, to_timeout(hour: 1))

  @doc """
  Fetches a value from its owner's share, or returns why it could not.

  `:l2_error` covers a failure or timeout of the owner, and `:l2_skipped` a disabled level.
  """
  @spec lookup(term()) :: result()
  def lookup(cache_key) do
    if enabled?() do
      case __MODULE__.fetch(cache_key, timeout: timeout()) do
        {:ok, _value} = hit -> hit
        {:error, %Nebulex.KeyError{}} -> :miss
        {:error, _reason} -> :l2_error
      end
    else
      :l2_skipped
    end
  rescue
    _exception -> :l2_error
  catch
    :exit, _reason -> :l2_error
  end

  @doc """
  Writes a value to its owner's share in a separate task, unless its key-value is tombstoned
  there.
  """
  @spec fill(term(), term()) :: :ok
  def fill(cache_key, value) do
    _ = Tasks.start_child(fn -> fill_owner(cache_key, value) end)
    :ok
  end

  defp fill_owner(cache_key, value) do
    :telemetry.span(@fill_event, %{}, fn ->
      result =
        with {:ok, owner} <- __MODULE__.find_node(cache_key) do
          RPC.call(owner, __MODULE__, :fill_share, [cache_key, value, ttl()], timeout())
        end

      {:ok, %{result: fill_result(result)}}
    end)
  rescue
    _exception -> :ok
  catch
    :exit, _reason -> :ok
  end

  defp fill_result({:error, _reason}), do: :error
  defp fill_result(put_or_dropped), do: put_or_dropped

  @doc false
  @spec fill_share(term(), term(), timeout()) :: :put | :dropped_stale
  def fill_share({:lookup, [user_id, key, _accessor_path]} = cache_key, value, ttl) do
    primary = __MODULE__.__primary__()
    :ok = primary.put(cache_key, value, ttl: ttl)

    if Tombstones.Cache.tombstoned?(KeyValues.Cache, {user_id, key}) do
      :ok = primary.delete(cache_key)
      :dropped_stale
    else
      :put
    end
  end

  @doc """
  Deletes a key from this node's share, returning whether it was there.
  """
  @spec delete_local(term()) :: boolean()
  def delete_local(cache_key) do
    enabled?() and match?({:ok, _value}, __MODULE__.__primary__().take(cache_key))
  rescue
    _exception -> false
  catch
    :exit, _reason -> false
  end

  @doc """
  Returns the lookup keys of a key-value held in this node's share, accessor variants included.
  """
  @spec local_lookup_keys(integer(), String.t()) :: [term()]
  def local_lookup_keys(user_id, key) do
    if enabled?() do
      accessor_paths =
        __MODULE__.__primary__().get_all!(
          query: [{{:entry, {:lookup, [user_id, key, :"$1"]}, :_, :_, :_, :_}, [], [:"$1"]}]
        )

      Enum.map(accessor_paths, &{:lookup, [user_id, key, &1]})
    else
      []
    end
  rescue
    _exception -> []
  catch
    :exit, _reason -> []
  end

  @doc """
  Deletes every entry on every node.
  """
  @spec clear() :: :ok
  def clear do
    if enabled?(), do: __MODULE__.delete_all()
    :ok
  rescue
    _exception -> :ok
  catch
    :exit, _reason -> :ok
  end

  defp settings, do: Application.get_env(:logflare, Logflare.KeyValues.Cache, [])
end
