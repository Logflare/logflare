defmodule Logflare.TestSupport.AdaptorTelemetryPipeline do
  use Broadway

  alias Broadway.Message

  @spec start_link(keyword()) :: Broadway.on_start()
  def start_link(opts) do
    Broadway.start_link(__MODULE__, opts)
  end

  @impl Broadway
  def handle_message(_processor, message, _context) do
    Message.put_batcher(message, :default)
  end

  @impl Broadway
  def handle_batch(_batcher, messages, _batch_info, _context), do: messages
end
