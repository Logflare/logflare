defmodule Logflare.Logs.LogEvents.Cache do
  @moduledoc false

  @behaviour Logflare.Cache
  @behaviour Logflare.ContextCache

  alias Logflare.Cache.CachexOps
  alias Logflare.ContextCache
  alias Logflare.LogEvent, as: LE
  alias Logflare.Logs.LogEvents

  @cache __MODULE__

  def child_spec(_) do
    CachexOps.child_spec(@cache,
      limit: 15_000,
      ttl: to_timeout(minute: 10),
      purge_interval: to_timeout(minute: 5),
      compressed: true
    )
  end

  @impl Logflare.Cache
  def healthy?, do: CachexOps.healthy?(__MODULE__)

  @impl Logflare.Cache
  def stats, do: CachexOps.stats(__MODULE__)

  @impl Logflare.Cache
  def reset, do: CachexOps.reset(__MODULE__)

  @impl ContextCache
  def fetch(key, getter), do: CachexOps.fetch(__MODULE__, key, getter)

  @impl ContextCache
  def update(key, value), do: CachexOps.update(__MODULE__, key, value)

  @impl ContextCache
  def keys_to_bust(kw), do: CachexOps.keys_to_bust(__MODULE__, kw)

  @impl ContextCache
  def delete_keys(keys), do: CachexOps.delete_keys(__MODULE__, keys)

  @fetch_event_by_id {:fetch_event_by_id, 2}
  @spec fetch_event_by_id(atom, binary(), Keyword.t()) :: {:ok, LE.t() | nil} | {:error, map()}
  def fetch_event_by_id(source_token, id) when is_atom(source_token) and is_binary(id) do
    ContextCache.apply_fun(LogEvents, @fetch_event_by_id, [source_token, id])
  end

  def fetch_event_by_id(source_token, id, opts) when is_atom(source_token) and is_binary(id) do
    ContextCache.apply_fun(LogEvents, @fetch_event_by_id, [source_token, id, opts])
  end

  @spec put(atom(), String.t(), LE.t()) :: {:error, boolean} | {:ok, boolean}
  def put(source_token, key, log_event) do
    Cachex.put(__MODULE__, {source_token, key}, log_event)
  end

  @spec get(atom(), String.t()) :: {:ok, LE.t() | nil}
  def get(source_token, log_id) do
    Cachex.get(__MODULE__, {source_token, log_id})
  end

  def name, do: __MODULE__
end
