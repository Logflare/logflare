defmodule Logflare.Source.BigQuery.BufferProducer do
  @moduledoc false
  use GenStage

  require Logger

  alias Logflare.Source.BigQuery.BufferCounter
  alias Logflare.LogEvent, as: LE

  @impl true
  def init(%{source_id: source_id}) when is_atom(source_id) do
    {:producer,
     %{
       demand: 0,
       source_id: source_id
     }}
  end

  @impl true
  def handle_demand(incoming_demand, %{demand: demand} = state) do
    handle_receive_messages(%{state | demand: demand + incoming_demand})
  end

  @impl true
  def handle_info(:receive_messages, state) do
    handle_receive_messages(%{state | receive_timer: nil})
  end

  @impl true
  def handle_info(_, state) do
    {:noreply, [], state}
  end

  @spec ack(atom(), [%Broadway.Message{}], [%Broadway.Message{}]) :: :ok
  def ack(source_id, successful, unsuccessful) when is_atom(source_id) do
    Enum.each(successful, fn %{data: %LE{}} = message ->
      BufferCounter.ack(source_id, message.data.id)
    end)

    Enum.each(unsuccessful, fn %{data: %LE{}} = message ->
      BufferCounter.ack(source_id, message.data.id)
    end)

    :ok
  end

  defp handle_receive_messages(
         %{source_id: source_id, receive_timer: nil, demand: demand} = state
       )
       when demand > 0 do
    Buffer.pop(source_id, demand)
    {:noreply, [], %{state | demand: 0}}
  end

  defp handle_receive_messages(state) do
    {:noreply, [], state}
  end

  @impl true
  def terminate(reason, _state) do
    Logger.info("Going Down - #{inspect(reason)} - #{__MODULE__}")
    reason
  end
end
