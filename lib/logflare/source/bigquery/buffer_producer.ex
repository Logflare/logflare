defmodule Logflare.Source.BigQuery.BufferProducer do
  @moduledoc false
  use GenStage

  require Logger

  alias Logflare.Source.BigQuery.BufferCounter

  @impl true
  def init(%{source_id: token}), do: init(%{source_token: token})

  def init(%{source_token: source_token}) when is_atom(source_token) do
    {:producer,
     %{
       demand: 0,
       source_token: source_token
     }, buffer_size: 10_000}
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
    {:noreply, [], state, :hibernate}
  end

  @spec ack(atom(), [Broadway.Message.t()], [Broadway.Message.t()]) :: :ok
  def ack(source_token, successful, unsuccessful) when is_atom(source_token) do
    BufferCounter.ack_batch(source_token, successful ++ unsuccessful)

    :ok
  end

  defp handle_receive_messages(
         %{source_token: _source_token, receive_timer: nil, demand: demand} = state
       )
       when demand > 0 do
    # would normall pop log events from a buffer here

    {:noreply, [], %{state | demand: 0}, :hibernate}
  end

  defp handle_receive_messages(state) do
    {:noreply, [], state, :hibernate}
  end

  @impl true
  def terminate(reason, _state) do
    Logger.info("Going Down - #{inspect(reason)} - #{__MODULE__}")
    reason
  end
end
