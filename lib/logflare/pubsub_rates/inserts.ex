defmodule Logflare.PubSubRates.Inserts do
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
    PubSub.subscribe(Logflare.PubSub, "inserts")

    {:ok, state}
  end

  def handle_info({:inserts, source_id, inserts}, state) do
    Cache.cache_inserts(source_id, inserts)
    {:noreply, state}
  end
end
