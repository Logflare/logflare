defmodule BroadwayETS.Producer do
  @moduledoc """
  A GenStage producer that continuously receives messages from a SQS queue and
  acknowledge them after being successfully processed.
  ## Options using ExAwsClient (Default)
    * `:queue_name` - Required. The name of the queue.
    * `:max_number_of_messages` - Optional. The maximum number of messages to be fetched
      per request. This value must be between `1` and `10`, which is the maximun number
      allowed by AWS. Default is `10`.
    * `:wait_time_seconds` - Optional. The duration (in seconds) for which the call waits
      for a message to arrive in the queue before returning.
    * `:config` - Optional. A set of options that overrides the default ExAws configuration
      options. The most commonly used options are: `:access_key_id`, `:secret_access_key`,
      `:scheme`, `:region` and `:port`. For a complete list of configuration options and
      their default values, please see the `ExAws` documentation.
  ## Additional options
    * `:sqs_client` - Optional. A module that implements the `BroadwaySQS.SQSClient`
      behaviour. This module is responsible for fetching and acknowledging the
      messages. Pay attention that all options passed to the producer will be forwarded
      to the client. It's up to the client to normalize the options it needs. Default
      is `ExAwsClient`.
    * `:receive_interval` - Optional. The duration (in milliseconds) for which the producer
      waits before making a request for more messages. Default is 5000.
  ### Example
      Broadway.start_link(MyBroadway,
        name: MyBroadway,
        producers: [
          default: [
            module: {BroadwaySQS.Producer,
              queue_name: "my_queue",
              config: [
                access_key_id: "YOUR_AWS_ACCESS_KEY_ID",
                secret_access_key: "YOUR_AWS_SECRET_ACCESS_KEY"
              ]
            }
          ]
        ]
      )
  The above configuration will set up a producer that continuously receives messages from `"my_queue"`
  and sends them downstream.
  """

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
    Logger.info("Deleted messages")

    Enum.each(successful, fn _message ->
      # [object] = message.data
      TableBuffer.ack(table)
    end)
  end

  defp receive_messages_from_buffer(state, _total_demand) do
    {opts} = state.table_name
    table = opts[:table_name]

    event_message = TableBuffer.pop(table)

    case event_message do
      :empty ->
        []

      _ ->
        IO.inspect(event_message, label: "POP")

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
