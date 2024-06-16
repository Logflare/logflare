defmodule Logflare.PubSubRates.Buffers do
  @moduledoc false
  alias Logflare.PubSubRates
  alias Logflare.Sources
  alias Logflare.Backends

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
    source = Sources.Cache.get_by_id(source_token)

    if source do
      Backends.set_buffer_len(source, nil, buffers)
    end

    {:noreply, state}
  end

  def handle_info({:buffers, source_token, backend_token, buffers}, state) do
    source = Sources.Cache.get_by_id(source_token)
    backend = Backends.Cache.get_backend_by(token: backend_token)

    if source do
      Backends.set_buffer_len(source, backend, buffers)
    end

    {:noreply, state}
  end
end
