defmodule Logflare.Backends.Adaptor.ConsolidatedWebhookAdaptor.EncodedEvent do
  @moduledoc """
  A JSON-encoded event body and the queue pointer that owns it.

  The processor stores this representation in the generation table in place of the
  `Logflare.LogEvent`, so a retry sends the same bytes without a second encode.
  """

  alias Logflare.Backends.IngestEventQueue.LogEventPointer

  @enforce_keys [:pointer, :json]
  defstruct [:pointer, :json]

  @type t :: %__MODULE__{
          pointer: LogEventPointer.t(),
          json: binary()
        }
end
