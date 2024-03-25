defmodule Logflare.PubSubRates.Rates do
  @moduledoc false
  alias Logflare.PubSubRates.Cache
  alias Logflare.PubSubRates

  require Logger

  use GenServer

  def start_link(args \\ []) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init(state) do
    PubSubRates.subscribe(:rates)
    {:ok, state}
  end

  def handle_info({:rates, source_token, rates}, state) do
    Cache.cache_rates(source_token, rates)
    {:noreply, state}
  end
end
