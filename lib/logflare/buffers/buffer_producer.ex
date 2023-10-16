defmodule Logflare.Buffers.BufferProducer do
  @moduledoc """
  A generic buffer producer
  """
  use GenStage

  alias Logflare.Buffers.Buffer

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  def init(opts) do
    state = Enum.into(opts, %{buffer_module: nil, buffer_pid: nil, demand: 0})

    for {key, nil} <- state do
      raise "#{key} must be provided and cannot be nil"
    end

    loop()
    {:producer, state}
  end

  def handle_info(:resolve, state) do
    {items, state} = resolve_demand(state)
    loop()
    {:noreply, items, state}
  end

  defp loop, do: Process.send_after(self(), :resolve, 250)

  def handle_demand(demand, state) do
    {items, state} = resolve_demand(state, demand)
    {:noreply, items, state}
  end

  defp resolve_demand(
         %{demand: prev_demand} = state,
         new_demand \\ 0
       ) do
    total_demand = prev_demand + new_demand
    {:ok, items} = Buffer.pop_many(state.buffer_module, state.buffer_pid, total_demand)

    {items, %{state | demand: total_demand - length(items)}}
  end
end
