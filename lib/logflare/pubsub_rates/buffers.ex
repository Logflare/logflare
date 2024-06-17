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

  def init(state) do
    PubSubRates.subscribe(:buffers)
    {:ok, state}
  end

  def handle_info({:buffers, source_token, buffers}, state) when is_map(buffers) do
    Cache.cache_buffers(source_token, nil, buffers)
    {:noreply, state}
  end

  def handle_info({:buffers, source_token, backend_token, buffers}, state) when is_map(buffers) do
    Cache.cache_buffers(source_token, backend_token, buffers)
    {:noreply, state}
  end
end
