defmodule Logflare.Logs.LogEvents.Cache do
  @moduledoc false
  alias Logflare.Logs.LogEvents
  alias Logflare.ContextCache
  alias Logflare.LogEvent, as: LE
  alias Logflare.Utils

  @cache __MODULE__

  def child_spec(_) do
    stats = Application.get_env(:logflare, :cache_stats, false)

    %{
      id: __MODULE__,
      name: __MODULE__,
      start: {
        Cachex,
        :start_link,
        [
          @cache,
          [
            hooks:
              [
                if(stats, do: Utils.cache_stats()),
                Utils.cache_limit(15_000)
              ]
              |> Enum.filter(& &1),
            expiration: Utils.cache_expiration_min(10),
            compressed: true
          ]
        ]
      }
    }
  end

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

  @spec get(atom(), String.t()) ::{:ok, LE.t() | nil}
  def get(source_token, log_id) do
    Cachex.get(__MODULE__, {source_token, log_id})
  end

  def name, do: __MODULE__
end
