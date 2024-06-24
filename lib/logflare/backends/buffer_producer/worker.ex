defmodule Logflare.Backends.BufferProducer.Worker do
  @moduledoc """
  A generic broadway producer that doesn't actually produce anything.

  Meant for push through Broadway.push_messages/2
  """
  use GenStage
  alias Logflare.Source
  alias Logflare.PubSubRates
  require Logger

  @active_broadcast_interval 1000
  @idle_broadcast_interval 5000

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def init(state) do
    state =
      Map.merge(
        %{
          last_len: 0,
          active_broadcast_interval: @active_broadcast_interval,
          idle_broadcast_interval: @idle_broadcast_interval
        },
        state
      )

    Process.send_after(self(), :check_len, state.active_broadcast_interval)
    Process.send_after(self(), :periodic_broadcast, state.idle_broadcast_interval)
    {:ok, state}
  end

  def handle_info(:check_len, state) do
    state = maybe_global_broadcast_producer_buffer_len(state)
    {:noreply, state}
  end

  def handle_info(:periodic_broadcast, state) do
    local_broadcast_cluster_length(state)
    Process.send_after(self(), :periodic_broadcast, state.idle_broadcast_interval)
    {:noreply, state}
  end

  defp maybe_global_broadcast_producer_buffer_len(state) do
    len = GenStage.estimate_buffered_count(state.producer_pid)
    Logger.debug("BufferProducer.Worker - #{state.source_token} - #{len} buffer len")

    {should_broadcast?, state} =
      if state.last_len == 0 and len == 0 do
        Logger.debug("BufferProducer.Worker - #{state.source_token} - Idle buffer len checking")
        Process.send_after(self(), :check_len, state.idle_broadcast_interval)
        {false, state}
      else
        Logger.debug("BufferProducer.Worker - #{state.source_token} - Active buffer len checking")
        Process.send_after(self(), :check_len, state.active_broadcast_interval)
        {true, %{state | last_len: len}}
      end

    # broadcast length
    if should_broadcast? do
      local_buffer = %{Node.self() => %{len: len}}

      cluster_broadcast_payload =
        if state.backend_token do
          # cache local length
          PubSubRates.Cache.cache_buffers(state.source_token, state.backend_token, local_buffer)

          {:buffers, state.source_token, state.backend_token, local_buffer}
        else
          PubSubRates.Cache.cache_buffers(state.source_token, nil, local_buffer)
          {:buffers, state.source_token, local_buffer}
        end

      Logger.debug(
        "BufferProducer.Worker - #{state.source_token} - Broadcasting buffer len globally"
      )

      Phoenix.PubSub.broadcast(
        Logflare.PubSub,
        "buffers",
        cluster_broadcast_payload
      )
    end

    state
  end

  defp local_broadcast_cluster_length(state) do
    # broadcasts cluster buffer length to local channels
    cluster_buffer_len = PubSubRates.Cache.get_cluster_buffers(state.source_token)

    payload = %{
      buffer: cluster_buffer_len,
      source_token: state.source_token,
      backend_token: state.backend_token
    }

    Source.ChannelTopics.local_broadcast_buffer(payload)
  end
end
