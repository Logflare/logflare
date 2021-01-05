defmodule Logflare.Sources.BuffersCache do
  @moduledoc false
  import Cachex.Spec
  use Logflare.Commons

  @ttl :timer.hours(1)
  @cache __MODULE__

  def child_spec(_) do
    %{
      id: __MODULE__,
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

  def put_read_receipt(%LE{} = log_event) do
    Cachex.put(@cache, log_event.id, log_event, ttl: @ttl)
  end

  def take_read_receipt(log_event_id) do
    Cachex.take(@cache, log_event_id)
  end
end
