defmodule Logflare.Buffers.BufferProducer do
  @moduledoc """
  A generic producer that acts as a producer but doesn't actually produce anything.
  """
  use GenStage

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  def init(opts) do
    state = Enum.into(opts, %{buffer_module: nil, buffer_pid: nil, demand: 0})
    {:producer, state}
  end

  def handle_info(:resolve, state) do
    {items, state} = resolve_demand(state)
    {:noreply, items, state}
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
end
