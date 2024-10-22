defmodule Logflare.PubSubRates.Rates do
  @moduledoc false
  alias Logflare.PubSubRates.Cache
  alias Logflare.PubSubRates

  require Logger

  use GenServer

  def child_spec(args) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [args]},
      type: :worker,
      restart: :permanent,
      shutdown: 500
    }
  end

  def start_link(args \\ []) do
    partition = Keyword.get(args, :partition, 0)
    name = :"#{__MODULE__}#{partition}"

    GenServer.start_link(__MODULE__, args, name: name)
  end

  def init(state) do
    partition = Keyword.get(state, :partition, 0)
    topic = "rates#{partition}"
    PubSubRates.subscribe(topic)
    {:ok, state}
  end

  def handle_info({"rates", source_token, rates}, state) do
    Cache.cache_rates(source_token, rates)
    {:noreply, state}
  end
end
