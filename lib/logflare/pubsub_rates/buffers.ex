defmodule Logflare.PubSubRates.Buffers do
  @moduledoc """
  Subscribes to all incoming cluster messages of each node's buffer.
  """
  alias Logflare.PubSubRates.Cache
  alias Logflare.PubSubRates

  require Logger

  use GenServer

  @topic "buffers"

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

  def handle_info({@topic, _source_token, _buffers} = msg, state) do
    {:noreply, state}
  end

  def handle_info({@topic, source_id, backend_id, buffers}, state) do
    Cache.cache_buffers(source_id, backend_id, buffers)
    {:noreply, state}
  end

  defp get_partition_opt(args) do
    Keyword.get(args, :partition, "0")
  end
end
