defmodule Logflare.PubSubRates.Buffers do
  @moduledoc """
  Subscribes to all incoming cluster messages of each node's buffer.
  """
  alias Logflare.PubSubRates.Cache
  alias Logflare.PubSubRates

  require Logger

  use GenServer

  def start_link(args \\ []) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  @impl GenServer
  def init(_state) do
    PubSubRates.subscribe("buffers")
    {:ok, %{}}
  end

  @impl GenServer
  def handle_info({"buffers", source_id, backend_id, buffers}, state)
      when is_integer(source_id) and is_map(buffers) do
    Cache.cache_buffers(source_id, backend_id, buffers)
    {:noreply, state}
  end

  # TODO: remove in >v1.8.x
  @impl GenServer
  def handle_info(_, state) do
    # don't handle old format of 3-elem or 4-elem tuples.
    {:noreply, state}
  end
end
