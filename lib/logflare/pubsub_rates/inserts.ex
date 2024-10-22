defmodule Logflare.PubSubRates.Inserts do
  @moduledoc false
  alias Logflare.PubSubRates
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
    PubSubRates.subscribe("inserts")
    {:ok, state}
  end

  def handle_info({"inserts", source_token, inserts}, state) do
    Cache.cache_inserts(source_token, inserts)
    {:noreply, state}
  end
end
