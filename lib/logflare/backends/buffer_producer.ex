defmodule Logflare.Backends.BufferProducer do
  @moduledoc """
  A generic broadway producer that doesn't actually produce anything.

  Meant for push through Broadway.push_messages/2
  """
  use GenStage
  alias Logflare.Source
  alias Logflare.PubSubRates

  @default_broadcast_interval 5_000

  def start_link(opts) when is_list(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  def init(opts) do
    state =
      Enum.into(opts, %{
        buffer_module: nil,
        buffer_pid: nil,
        demand: 0,
        # TODO: broadcast by id instead.
        source_token: nil,
        backend_token: nil,
        broadcast_interval: @default_broadcast_interval
      })

    loop(state.broadcast_interval)
    {:producer, state, buffer_size: 10_000}
  end

  def handle_info(:resolve, state) do
    {items, state} = resolve_demand(state)
    {:noreply, items, state}
  end

  def handle_info(:broadcast, state) do
    do_async_broadcast(state)
    loop(state.broadcast_interval)
    {:noreply, [], state}
  end

  def handle_info({:update_state, new_state}, _state) do
    {:noreply, [], new_state}
  end

  def handle_info({:add_to_buffer, items}, state) do
    {:noreply, items, state}
  end

  defp do_async_broadcast(state) do
    pid = self()

    Task.start_link(fn ->
      pool_size = Application.get_env(:logflare, Logflare.PubSub)[:pool_size]
      len = GenStage.estimate_buffered_count(pid)
      local_buffer = %{Node.self() => %{len: len}}

      shard = :erlang.phash2(state.source_token, pool_size)

      cluster_buffer = PubSubRates.Cache.get_cluster_buffers(state.source_token)

      # maybe broadcast
      payload = %{
        buffer: cluster_buffer,
        source_token: state.source_token,
        backend_token: state.backend_token
      }

      # cluster broadcast
      cluster_broadcast_payload =
        if state.backend_token do
          {:buffers, state.source_token, state.backend_token, local_buffer}
        else
          {:buffers, state.source_token, local_buffer}
        end

      Phoenix.PubSub.broadcast(
        Logflare.PubSub,
        "buffers:shard-#{shard}",
        cluster_broadcast_payload
      )

      # channel broadcast
      Source.ChannelTopics.broadcast_buffer(payload)
    end)
  end

  def handle_demand(demand, state) do
    {items, state} = resolve_demand(state, demand)
    {:noreply, items, state}
  end

  defp resolve_demand(
         %{demand: prev_demand} = state,
         new_demand \\ 0
       ) do
    total_demand = prev_demand + new_demand
    {[], %{state | demand: total_demand}}
  end

  defp loop(interval) do
    Process.send_after(self(), :broadcast, interval)
  end
end
