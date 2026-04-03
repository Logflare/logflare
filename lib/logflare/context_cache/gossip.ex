defmodule Logflare.ContextCache.Gossip do
  @moduledoc false

  require Logger
  require Cachex.Spec

  alias Logflare.Cluster.Utils, as: ClusterUtils
  alias Logflare.ContextCache
  alias Logflare.ContextCache.Tombstones

  @telemetry_handler_id "context-cache-gossip-logger"

  # this logger can be attached to provide visibility into gossip decisions and dropped multicasts
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
  def handle_telemetry_event(event, measurements, metadata, _config) do
    case event do
      [:logflare, :context_cache_gossip, :multicast, :stop] ->
        %{action: action, cache: cache, key: key} = metadata

        duration = System.convert_time_unit(measurements.duration, :native, :millisecond)

        msg =
          case action do
            :done ->
              "Multicasted gossip for #{cache} #{inspect(key)} to peer nodes in #{duration}ms"

            :disabled ->
              "Context cache gossip is disabled, skipping multicast for #{cache} #{inspect(key)} in #{duration}ms"

            :ignore ->
              "Skipped gossip for #{cache} #{inspect(key)} in #{duration}ms"
          end

        Logger.debug(msg)

      [:logflare, :context_cache_gossip, :receive, :stop] ->
        %{action: action, cache: cache, key: key} = metadata

        duration = System.convert_time_unit(measurements.duration, :native, :millisecond)

        case action do
          :dropped_no_pkey ->
            Logger.warning("""
            Dropped gossip for #{cache} #{inspect(key)} in #{duration}ms: no primary keys \
            could be extracted from the value, so staleness cannot be determined\
            """)

          :dropped_stale ->
            Logger.warning("""
            Dropped gossip for #{cache} #{inspect(key)} in #{duration}ms: tombstone cache indicates \
            this record was recently updated or deleted, so the incoming gossip is likely stale\
            """)

          :cached ->
            Logger.debug("Cached gossip for #{cache} #{inspect(key)} in #{duration}ms")

          :refreshed ->
            Logger.debug("Refreshed gossip for #{cache} #{inspect(key)} in #{duration}ms")
        end
    end
  end

  def multicast(Cachex.Spec.cache(name: cache), key, value) do
    multicast(cache, key, value)
  end

  def multicast(cache, key, value) when is_atom(cache) do
    meta = %{cache: cache, key: key, value: value}

    :telemetry.span([:logflare, :context_cache_gossip, :multicast], meta, fn ->
      action = do_multicast(cache, key, value)
      {:ok, Map.put(meta, :action, action)}
    end)
  end

  # Negative lookups (`nil` or `[]`) are not gossiped. If Node A caches `nil`,
  # and the record is immediately created, a delayed `nil` cast to Node B
  # would cause phantom "not found" lookups while the record actually exists in the database.
  defp do_multicast(_cache, _key, nil), do: :ignore
  defp do_multicast(_cache, _key, []), do: :ignore

  # Explicitly ignore high-volume/ephemeral caches
  defp do_multicast(Logflare.Logs.LogEvents.Cache, _key, _value), do: :ignore

  # Ignore caches with complicated or missing primary key structures, where staleness cannot be reliably detected
  defp do_multicast(Logflare.Auth.Cache, _key, _value), do: :ignore
  defp do_multicast(Logflare.Rules.Cache, _key, _value), do: :ignore

  defp do_multicast(cache, key, value) when is_atom(cache) do
    %{enabled: enabled, ratio: ratio, max_nodes: max_nodes} =
      Application.fetch_env!(:logflare, :context_cache_gossip)

    if enabled do
      peers = ClusterUtils.peer_list_partial(ratio, max_nodes)
      :erpc.multicast(peers, __MODULE__, :receive_gossip, [cache, key, value])
      :done
    else
      :disabled
    end
  end

  @doc false
  def receive_gossip(cache, key, value) do
    meta = %{cache: cache, key: key, value: value}

    :telemetry.span([:logflare, :context_cache_gossip, :receive], meta, fn ->
      action = do_receive_gossip(cache, key, value)
      {:ok, Map.put(meta, :action, action)}
    end)
  end

  defp do_receive_gossip(cache, key, value) do
    if Cachex.exists?(cache, key) == {:ok, true} do
      # refresh if the node already has this cache key
      Cachex.refresh(cache, key)
      :refreshed
    else
      pkeys = pkeys_from_cached_value(value)

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
  end

  defp pkeys_from_cached_value(values) when is_list(values) do
    Enum.flat_map(values, &pkeys_from_cached_value/1)
  end

  defp pkeys_from_cached_value({:ok, value}), do: pkeys_from_cached_value(value)
  defp pkeys_from_cached_value(%{id: id}), do: [id]
  defp pkeys_from_cached_value(_value), do: []

  @doc false
  def record_tombstones(context_pkeys) when is_list(context_pkeys) do
    # Writes a short-lived marker for a primary key indicating it was recently updated or deleted.
    # Incoming cache multicasts check this tombstone cache to determine if their payload could be stale.
    Enum.each(context_pkeys, fn
      # don't need to tombstone new records
      {_context, :not_found} ->
        :ignore

      {context, pkey} ->
        if pkey = format_busted_pkey(pkey) do
          cache = ContextCache.cache_name(context)
          Tombstones.Cache.put_tombstone(cache, pkey)
        end
    end)
  end

  defp format_busted_pkey(pkey) when is_integer(pkey) or is_binary(pkey), do: pkey

  defp format_busted_pkey(info) when is_list(info) do
    info |> Keyword.get(:id) |> format_busted_pkey()
  end

  defp format_busted_pkey(%{id: id}), do: format_busted_pkey(id)
  defp format_busted_pkey(_), do: nil
end
