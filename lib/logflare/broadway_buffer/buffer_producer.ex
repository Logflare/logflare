defmodule BroadwayBuffer.Producer do
  use GenStage

  require Logger

  alias Logflare.TableBuffer

  @default_receive_interval 1000

  @impl true
  def init(opts) do
    {:producer,
     %{
       demand: 0,
       receive_timer: nil,
       receive_interval: @default_receive_interval,
       table_name: {opts}
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

  def ack(table, successful, _unsuccessful) do
    Enum.each(successful, fn _message ->
      # [object] = message.data
      TableBuffer.ack(table)
    end)
  end

  defp receive_messages_from_buffer(state, _total_demand) do
    {opts} = state.table_name
    table = opts[:table_name]

    pop = TableBuffer.pop(table)

    case pop do
      :empty ->
        []

      _ ->
        {_time_event, event} = pop
        event_message = %{table: table, event: event}

        [
          %Broadway.Message{
            data: event_message,
            acknowledger: {__MODULE__, table, "unsuccessful"}
          }
        ]
    end
  end

  defp schedule_receive_messages(interval) do
    Process.send_after(self(), :receive_messages, interval)
  end
end
