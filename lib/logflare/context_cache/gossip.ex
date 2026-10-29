defmodule Logflare.ContextCache.Gossip do
  @moduledoc """
  Distributes cache entries across cluster nodes.

  When a local cache miss occurs and the value is fetched, this module can be
  used to multicast the data to a subset of peer nodes. This populates peer caches
  to improve cluster-wide hit rates and reduce database load.

  ### Default Logging Handler

  This module provides a default Telemetry handler that logs "multicast" and "receive"
  events at appropriate log levels. To enable this logging, call `attach_logger/0`.
  To disable it, call `detach_logger/0`.
  """

  require Logger
  require Cachex.Spec

  alias Logflare.Cluster.Utils, as: ClusterUtils
  alias Logflare.ContextCache
  alias Logflare.ContextCache.Tombstones

  @telemetry_handler_id "context-cache-gossip-logger"

  @doc """
  Attaches a default Telemetry handler for logging.
  """
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

  @doc """
  Undoes `attach_logger/0` by detaching the attached logger.
  """
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

        Logger.notice(msg)

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
            Logger.notice("Cached gossip for #{cache} #{inspect(key)} in #{duration}ms")

          :refreshed ->
            Logger.notice("Refreshed gossip for #{cache} #{inspect(key)} in #{duration}ms")
        end
    end
  end

  @doc """
  Distributes a cache key and value to a subset of cluster peers.

  It filters out:
  - `nil` or empty list values to prevent phantom "not found" states.
  - High-volume or ephemeral caches.
  - Caches without clear primary key structures (auth and rules).
  """
  def multicast(cache, key, value)

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
      :erpc.multicast(peers, __MODULE__, :receive, [cache, key, value])
      :done
    else
      :disabled
    end
  end

  @doc false
  def receive(cache, key, value) do
    meta = %{cache: cache, key: key, value: value}

    :telemetry.span([:logflare, :context_cache_gossip, :receive], meta, fn ->
      action = do_receive(cache, key, value)
      {:ok, Map.put(meta, :action, action)}
    end)
  end

  defp do_receive(cache, key, value) do
    if Cachex.exists?(cache, key) == {:ok, true} do
      # refresh if the node already has this cache key
      Cachex.refresh(cache, key)
      :refreshed
    else
      case pkey_check(cache, value) do
        :no_pkey ->
          :dropped_no_pkey

        :tombstoned ->
          :dropped_stale

        :fresh ->
          Cachex.put(cache, key, {:cached, value})
          :cached
      end
    end
  end

  @doc """
  Whether `value` may be stale: it holds no primary key to check against tombstones, or one of its
  primary keys was tombstoned. The default `c:Logflare.ContextCache.stale_entry?/2`.
  """
  @spec stale_value?(module(), term()) :: boolean()
  def stale_value?(cache, value), do: pkey_check(cache, value) != :fresh

  defp pkey_check(cache, value) do
    case pkeys_from_cached_value(value) do
      [] -> :no_pkey
      pkeys -> if tombstoned_any?(cache, pkeys), do: :tombstoned, else: :fresh
    end
  end

  defp tombstoned_any?(cache, pkeys),
    do: Enum.any?(pkeys, &Tombstones.Cache.tombstoned?(cache, &1))

  defp pkeys_from_cached_value(values) when is_list(values) do
    Enum.flat_map(values, &pkeys_from_cached_value/1)
  end

  defp pkeys_from_cached_value({:ok, value}), do: pkeys_from_cached_value(value)
  defp pkeys_from_cached_value(%{id: id}), do: [id]
  defp pkeys_from_cached_value(_value), do: []

  @doc """
  Writes short-lived markers, given by the context cache's `c:Logflare.ContextCache.tombstones/1`,
  indicating a record was recently updated or deleted. Entries received from other nodes are checked
  against them to determine if they could be stale.
  """
  def record_tombstones(context_pkeys) when is_list(context_pkeys) do
    Enum.each(context_pkeys, fn
      # don't need to tombstone new records
      {_context, :not_found} ->
        :ignore

      {context, pkey_or_kw} ->
        cache = ContextCache.cache_name(context)

        pkey_or_kw
        |> cache.tombstones()
        |> Enum.each(&Tombstones.Cache.put_tombstone(cache, &1))
    end)
  end

  @doc """
  The primary key given as is, in an `:id` keyword or in a map with an `:id`. The default
  `c:Logflare.ContextCache.tombstones/1`.
  """
  @spec pkey_tombstones(term()) :: [term()]
  def pkey_tombstones(pkey_or_kw), do: pkey_or_kw |> format_busted_pkey() |> List.wrap()

  defp format_busted_pkey(pkey) when is_integer(pkey) or is_binary(pkey), do: pkey

  defp format_busted_pkey(info) when is_list(info) do
    info |> Keyword.get(:id) |> format_busted_pkey()
  end

  defp format_busted_pkey(%{id: id}), do: format_busted_pkey(id)
  defp format_busted_pkey(_), do: nil
end
