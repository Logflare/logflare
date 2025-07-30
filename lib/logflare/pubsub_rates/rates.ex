defmodule Logflare.PubSubRates.Rates do
  @moduledoc false
  alias Logflare.PubSubRates.Cache
  alias Logflare.PubSubRates

  require Logger

  use GenServer

  @topic "rates"

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
    partition = get_partition_opt(args)
    name = :"#{__MODULE__}#{partition}"

    GenServer.start_link(__MODULE__, args, name: name)
  end

  def init(args) do
    partition = get_partition_opt(args)

    PubSubRates.subscribe(@topic, partition)
    {:ok, args}
  end

  def handle_info({@topic, source_token, rates}, state) do
    Cache.cache_rates(source_token, rates)
    {:noreply, state}
  end

  defp get_partition_opt(args) do
    Keyword.get(args, :partition, "0")
  end
end
