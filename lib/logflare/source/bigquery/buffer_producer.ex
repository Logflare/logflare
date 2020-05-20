defmodule Logflare.Source.BigQuery.BufferProducer do
  @moduledoc false
  use GenStage

  require Logger

  alias Logflare.Source.BigQuery.Buffer
  alias Logflare.LogEvent, as: LE

  @default_receive_interval 5_000

  @impl true
  def init(%{source_id: source_id}) when is_atom(source_id) do
    {:producer,
     %{
       demand: 0,
       receive_timer: nil,
       receive_interval: @default_receive_interval,
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

  def handle_receive_messages(%{receive_timer: nil, demand: demand} = state) when demand > 0 do
    messages = receive_messages_from_buffer(state, demand)
    new_demand = demand - length(messages)

    receive_timer =
      case {messages, new_demand} do
        {[], _} -> schedule_receive_messages(state.receive_interval)
        {_, 0} -> nil
        _ -> schedule_receive_messages(0)
      end

    {:noreply, messages, %{state | demand: new_demand, receive_timer: receive_timer}}
  end

  def handle_receive_messages(state) do
    {:noreply, [], state}
  end

  def ack(table, successful, unsuccessful) do
    Enum.each(successful, fn %{data: %LE{}} = message ->
      Buffer.ack(table, message.data.id)
    end)

    Enum.each(unsuccessful, fn %{data: %LE{}} = message ->
      {:ok, le} = Buffer.ack(table, message.data.id)
      Buffer.push(le)
    end)
  end

  @impl true
  def terminate(reason, _state) do
    Logger.info("Going Down - #{inspect(reason)} - #{__MODULE__}")
    reason
  end

  defp receive_messages_from_buffer(%{source_id: source_id}, _total_demand) do
    source_id
    |> Buffer.pop()
    |> case do
      :empty ->
        []

      %LE{} = log_event ->
        [
          %Broadway.Message{
            data: log_event,
            acknowledger: {__MODULE__, source_id, "no idea what this does"}
          }
        ]
    end
  end

  defp schedule_receive_messages(interval) do
    Process.send_after(self(), :receive_messages, interval)
  end
end
