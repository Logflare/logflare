defmodule Logflare.PubSubRates.Buffers do
  @moduledoc false
  alias Phoenix.PubSub
  alias Logflare.PubSubRates.Cache

  require Logger

  use GenServer

  def start_link(args \\ []) do
    GenServer.start_link(
      __MODULE__,
      args,
      name: __MODULE__
    )
  end

  def init(state) do
    PubSub.subscribe(Logflare.PubSub, "buffers")

    {:ok, state}
  end

  def handle_info({:buffers, source_id, buffers}, state) do
    Cache.cache_buffers(source_id, buffers)
    {:noreply, state}
  end
end
