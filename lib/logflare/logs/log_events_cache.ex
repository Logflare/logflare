defmodule Logflare.Logs.LogEvents.Cache do
  @moduledoc false
  import Cachex.Spec
  alias Logflare.Logs.LogEvents
  alias Logflare.ContextCache
  alias Logflare.LogEvent, as: LE
  @ttl :timer.hours(24)

  @cache __MODULE__

  def child_spec(_) do
    %{
      id: __MODULE__,
      start: {
        Cachex,
        :start_link,
        [
          @cache,
          [expiration: expiration(default: @ttl), limit: limit(size: 100_000)]
        ]
      }
    }
  end

  @fetch_event_by_id_and_timestamp_key {:fetch_event_by_id_and_timestamp, 2}
  @spec put_event_with_id_and_timestamp(atom, keyword, LE.t()) :: term
  def put_event_with_id_and_timestamp(source_token, kw, %LE{} = log_event) do
    cache_key = {@fetch_event_by_id_and_timestamp_key, [source_token, kw]}
    Cachex.put(@cache, cache_key, {:ok, log_event}, ttl: @ttl)
  end

  @spec fetch_event_by_id_and_timestamp(atom, keyword) :: {:ok, map()} | {:error, map()}
  def fetch_event_by_id_and_timestamp(source_token, kw) when is_atom(source_token) do
    ContextCache.apply_fun(LogEvents, @fetch_event_by_id_and_timestamp_key, [source_token, kw])
  end

  @fetch_event_by_id {:fetch_event_by_id, 2}
  @spec fetch_event_by_id(atom, binary()) :: {:ok, LE.t() | nil} | {:error, map()}
  def fetch_event_by_id(source_token, id) when is_atom(source_token) and is_binary(id) do
    ContextCache.apply_fun(LogEvents, @fetch_event_by_id, [source_token, id])
  end

  @fetch_event_by_path {:fetch_event_by_path, 3}
  @spec fetch_event_by_path(atom, binary(), term()) :: {:ok, LE.t() | nil} | {:error, map()}
  def fetch_event_by_path(source_token, path, value)
      when is_atom(source_token) and is_binary(path) do
    ContextCache.apply_fun(LogEvents, @fetch_event_by_path, [source_token, path, value])
  end

  @spec put(atom(), term(), LE.t()) :: {:error, boolean} | {:ok, boolean}
  def put(source_token, key, log_event) do
    Cachex.put(__MODULE__, {source_token, key}, log_event)
  end

  @spec get!(atom(), term()) :: LE.t() | nil
  def get!(source_token, log_id) do
    Cachex.get!(__MODULE__, {source_token, log_id})
  end
end
