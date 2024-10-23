defmodule Logflare.PubSubRates.Inserts do
  @moduledoc false
  alias Logflare.PubSubRates
  alias Logflare.PubSubRates.Cache

  require Logger

  use GenServer

  @topic "inserts"

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
    topic = @topic <> partition
    PubSubRates.subscribe(topic)
    {:ok, args}
  end

  def handle_info({@topic, source_token, inserts}, state) do
    Cache.cache_inserts(source_token, inserts)
    {:noreply, state}
  end

  defp get_partition_opt(args) do
    Keyword.get(args, :partition, "0")
  end
end
