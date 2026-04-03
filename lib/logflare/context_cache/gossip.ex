defmodule Logflare.ContextCache.Gossip do
  @moduledoc false

  require Logger
  require Cachex.Spec

  alias Logflare.Cluster.Utils, as: ClusterUtils
  alias Logflare.ContextCache
  alias Logflare.ContextCache.Tombstones

  # Negative lookups (`nil` or `[]`) are not gossiped. If Node A caches `nil`,
  # and the record is immediately created, a delayed `nil` cast to Node B
  # would cause phantom "not found" lookups while the record actually exists in the database.
  def maybe_gossip(_cache, _key, nil), do: :ok
  def maybe_gossip(_cache, _key, []), do: :ok

  # Explicitly ignore high-volume/ephemeral caches
  def maybe_gossip(Logflare.Logs.LogEvents.Cache, _key, _value), do: :ok

  def maybe_gossip(Cachex.Spec.cache(name: cache), key, value) do
    maybe_gossip(cache, key, value)
  end

  def maybe_gossip(cache, key, value) when is_atom(cache) do
    meta = %{cache: cache, key: key}

    :telemetry.span([:logflare, :context_cache_gossip, :multicast], meta, fn ->
      %{enabled: enabled, ratio: ratio, max_nodes: max_nodes} =
        config = Application.fetch_env!(:logflare, :context_cache_gossip)

      if enabled do
        peers = ClusterUtils.peer_list_partial(ratio, max_nodes)
        :erpc.multicast(peers, __MODULE__, :receive_gossip, [cache, key, value])
      end

      {:ok, Map.merge(config, meta)}
    end)
  end

  @doc false
  def receive_gossip(cache, key, value) do
    meta = %{cache: cache, key: key}

    :telemetry.span([:logflare, :context_cache_gossip, :receive], meta, fn ->
      action =
        cond do
          # refresh if the node already has this cache key
          Cachex.exists?(cache, key) == {:ok, true} ->
            Cachex.refresh(cache, key)
            :refreshed

          # do nothing if the WAL recently busted this specific record
          tombstoned?(cache, value) ->
            :dropped_stale

          true ->
            Cachex.put(cache, key, {:cached, value})
            :cached
        end

      {:ok, Map.put(meta, :action, action)}
    end)
  end

  @doc false
  def record_tombstones(context_pkeys) when is_list(context_pkeys) do
    # Writes a short-lived marker for a primary key indicating it was recently updated or deleted.
    # Incoming cache broadcasts check this tombstone cache to determine if their payload could be stale.
    Enum.each(context_pkeys, fn context_pkey ->
      if tombstone = to_tombstone(context_pkey) do
        Tombstones.Cache.put_tombstone(tombstone)
      end
    end)
  end

  defp to_tombstone(context_pkey) do
    case context_pkey do
      {context, id} when is_integer(id) or is_binary(id) ->
        {ContextCache.cache_name(context), id}

      {context, info} when is_list(info) ->
        if id = Keyword.get(info, :id) do
          {ContextCache.cache_name(context), id}
        end

      {context, %{id: id}} ->
        {ContextCache.cache_name(context), id}

      _other ->
        Logger.warning(
          "Unable to create tombstone for context primary key: #{inspect(context_pkey)}"
        )

        nil
    end
  end

  defp tombstoned?(cache, value) do
    value
    |> extract_pkeys()
    |> Enum.any?(fn pkey -> Tombstones.Cache.tombstoned?(cache, pkey) end)
  end

  defp extract_pkeys(values) when is_list(values) do
    Enum.flat_map(values, &extract_pkeys/1)
  end

  defp extract_pkeys({:ok, value}), do: extract_pkeys(value)
  defp extract_pkeys(%{id: id}), do: [id]

  defp extract_pkeys(v) do
    Logger.warning("Unable to extract primary key from gossip value: #{inspect(v)}")
    []
  end
end
