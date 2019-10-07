defmodule Logflare.Sources.PubSub do
  @moduledoc """
  Handles distributed pub sub for source
  """
  def child_spec(_opts) do
    %{
      id: __MODULE__,
      start: {Redix.PubSub, :start_link, [name: __MODULE__]},
      type: :worker,
      restart: :permanent,
      shutdown: 500
    }
  end
end
