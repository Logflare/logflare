defmodule Logflare.PubSubRates do
  alias Phoenix.PubSub

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
    PubSub.subscribe(Logflare.PubSub, "source_rates")

    {:ok, state}
  end

  def handle_info({:rates, rates}, state) do
    IO.inspect(rates)
    {:noreply, state}
  end
end
