defmodule Logflare.Backends.CommonIngestPipeline do
  @moduledoc """
  A Broadway pipeline that handles all common log operations
  """
  use Broadway

  alias Broadway.Message
  alias Logflare.Backends
  alias Logflare.Buffers.BufferProducer
  alias Logflare.Buffers.MemoryBuffer
  alias Logflare.LogEvent
  alias Logflare.Source

  @spec start_link(Source.t()) :: {:ok, pid()}
  def start_link(%Source{} = source) do
    Broadway.start_link(__MODULE__,
      name: Backends.via_source(source, __MODULE__),
      producer: [
        module: {
          BufferProducer,
          buffer_module: MemoryBuffer, buffer_pid: Backends.via_source(source, :buffer)
        },
        transformer: {__MODULE__, :transform, []},
        concurrency: 1
      ],
      processors: [
        default: [concurrency: 1]
      ],
      batchers: [
        backends: [concurrency: 1, batch_size: 10]
      ],
      context: source
    )
  end

  # see the implementation for Backends.via_source/2 for how tuples are used to identify child processes
  @impl Broadway
  def process_name({:via, module, {registry, {id, pipeline}}}, base_name) do
    {:via, module, {registry, {id, pipeline, base_name}}}
  end

  @impl Broadway
  def handle_message(_processor_name, message, source) do
    message
    |> Message.update_data(&maybe_convert_to_log_event(&1, source))
    |> Message.put_batcher(:backends)
  end

  defp maybe_convert_to_log_event(%_{} = event, _source), do: event

  defp maybe_convert_to_log_event(%{} = params, source) do
    LogEvent.make(params, %{source: source})
  end

  @impl Broadway
  def handle_batch(:backends, messages, batch_info, source) do
    :telemetry.execute(
      [:logflare, :ingest, :common_pipeline, :handle_batch],
      %{batch_size: batch_info.size, batch_trigger: batch_info.trigger},
      %{
        source_token: source.token
      }
    )

    # dispatch messages to backends
    log_events = for %{data: msg} <- messages, do: msg
    Backends.dispatch_ingest(log_events, source)
    Backends.push_recent_logs(source, log_events)

    messages
  end

  # Broadway transformer for custom producer
  def transform(event, _opts) do
    %Message{
      data: event,
      acknowledger: {__MODULE__, :ack_id, :ack_data}
    }
  end

  def ack(_ack_ref, _successful, _failed) do
    # TODO: re-queue failed
  end
end
