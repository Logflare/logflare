defmodule Logflare.ContextCache.Gossip do
  @moduledoc false

  require Logger
  require Cachex.Spec

  alias Logflare.Cluster.Utils, as: ClusterUtils
  alias Logflare.ContextCache
  alias Logflare.ContextCache.Tombstones

  @telemetry_handler_id "context-cache-gossip-logger"

  def attach_logger do
    events = [
      [:logflare, :context_cache_gossip, :multicast, :stop],
      [:logflare, :context_cache_gossip, :receive, :stop]
    ]

    :telemetry.attach_many(
      @telemetry_handler_id,
      events,
      &__MODULE__.handle_telemetry_event/4,
      _no_config = []
    )
  end

  def detach_logger do
    :telemetry.detach(@telemetry_handler_id)
  end

  @doc false
  def handle_telemetry_event(event, _measurements, metadata, _config) do
    case {event, metadata} do
      {[:logflare, :context_cache_gossip, :receive, :stop], %{action: :dropped_no_pkey} = meta} ->
        %{cache: cache, key: key} = meta

        Logger.warning(
          "Dropped gossip for #{cache} #{inspect(key)}: no primary keys could be extracted from the value, so staleness cannot be determined"
        )

      _ ->
        :ok
    end
  end

  # Negative lookups (`nil` or `[]`) are not gossiped. If Node A caches `nil`,
  # and the record is immediately created, a delayed `nil` cast to Node B
  # would cause phantom "not found" lookups while the record actually exists in the database.
  def maybe_gossip(_cache, _key, nil), do: :ok
  def maybe_gossip(_cache, _key, []), do: :ok

  # Explicitly ignore high-volume/ephemeral caches
  def maybe_gossip(Logflare.Logs.LogEvents.Cache, _key, _value), do: :ok
  def maybe_gossip(Logflare.Auth.Cache, _key, _value), do: :ok
  def maybe_gossip(Logflare.Rules.Cache, _key, _value), do: :ok

  def maybe_gossip(Cachex.Spec.cache(name: cache), key, value) do
    maybe_gossip(cache, key, value)
  end

  def maybe_gossip(cache, key, value) when is_atom(cache) do
    meta = %{cache: cache, key: key, value: value}

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
    meta = %{cache: cache, key: key, value: value}

    :telemetry.span([:logflare, :context_cache_gossip, :receive], meta, fn ->
      action =
        if Cachex.exists?(cache, key) == {:ok, true} do
          # refresh if the node already has this cache key
          Cachex.refresh(cache, key)
          :refreshed
        else
          pkeys = extract_pkeys(value)

          cond do
            # if we can't extract any primary keys from the cache key/value,
            # we have no way to detect staleness, so we drop it to be safe
            pkeys == [] ->
              :dropped_no_pkey

            # do nothing if the WAL recently busted this specific record
            Enum.any?(pkeys, fn pkey -> Tombstones.Cache.tombstoned?(cache, pkey) end) ->
              :dropped_stale

            true ->
              Cachex.put(cache, key, {:cached, value})
              :cached
          end
        end

      {:ok, Map.put(meta, :action, action)}
    end)
  end

  @doc false
  def record_tombstones(context_pkeys) when is_list(context_pkeys) do
    # Writes a short-lived marker for a primary key indicating it was recently updated or deleted.
    # Incoming cache multicasts check this tombstone cache to determine if their payload could be stale.
    Enum.each(context_pkeys, fn
      # don't need to tombstone new records
      {_context, :not_found} ->
        :ignore

      {context, pkey} ->
        if pkey = to_tombstone_pkey(pkey) do
          cache = ContextCache.cache_name(context)
          Tombstones.Cache.put_tombstone(cache, pkey)
        end
    end)
  end

  defp to_tombstone_pkey(pkey) when is_integer(pkey) or is_binary(pkey), do: pkey

  defp to_tombstone_pkey(info) when is_list(info) do
    info |> Keyword.get(:id) |> to_tombstone_pkey()
  end

  defp to_tombstone_pkey(%{id: id}), do: to_tombstone_pkey(id)
  defp to_tombstone_pkey(_), do: nil

  defp extract_pkeys(values) when is_list(values) do
    Enum.flat_map(values, &extract_pkeys/1)
  end

  defp extract_pkeys({:ok, value}), do: extract_pkeys(value)
  defp extract_pkeys(%{id: id}), do: [id]
  defp extract_pkeys(_value), do: []
end
