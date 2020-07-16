defmodule Logflare.PubSubRates do
  alias Phoenix.PubSub
  alias Logflare.PubSubRates.Cache

  require Logger

  use GenServer

  def start_link() do
    GenServer.start_link(
      __MODULE__,
      [],
      name: __MODULE__
    )
  end

  def init(state) do
    PubSub.subscribe(Logflare.PubSub, "rates")
    PubSub.subscribe(Logflare.PubSub, "inserts")
    PubSub.subscribe(Logflare.PubSub, "buffers")

    {:ok, state}
  end

  def handle_info({:buffers, source_id, buffers}, state) do
    Cache.cache_buffers(source_id, buffers)
    {:noreply, state}
  end

  def handle_info({:inserts, source_id, inserts}, state) do
    Cache.cache_inserts(source_id, inserts)
    {:noreply, state}
  end

  def handle_info({:rates, source_id, rates}, state) do
    Cache.cache_rates(source_id, rates)
    {:noreply, state}
  end
end
