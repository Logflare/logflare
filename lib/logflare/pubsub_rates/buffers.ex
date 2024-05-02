defmodule Logflare.PubSubRates.Buffers do
  @moduledoc false
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

  def handle_info({:buffers, source_token, buffers}, state) do
    Cache.cache_buffers(source_token, buffers)
    {:noreply, state}
  end
end
