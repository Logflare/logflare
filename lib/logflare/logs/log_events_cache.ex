defmodule Logflare.Logs.LogEvents.Cache do
  @moduledoc false
  import Cachex.Spec
  alias Logflare.Logs.LogEvents
  alias Logflare.ContextCache
  @ttl :timer.hours(24)

  @cache __MODULE__

  def child_spec(_) do
    %{
      id: :cachex_logs_log_events_cache,
      start: {
        Cachex,
        :start_link,
        [
          @cache,
          [expiration: expiration(default: @ttl)]
        ]
      }
    }
  end

  @spec fetch_event_by_id_and_timestamp(Source.t(), keyword) :: {:ok, map()} | {:error, map()}
  def fetch_event_by_id_and_timestamp(source, kw) do
    ContextCache.apply_fun(LogEvents, __ENV__.function(), [source, kw])
  end
end
