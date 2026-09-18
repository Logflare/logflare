defmodule Logflare.Backends.Adaptor.WebhookV2Adaptor.EncodedEvent do
  @moduledoc """
  A JSON-encoded event body and the queue pointer that owns it.

  The processor stores this representation in the generation table in place of the
  `Logflare.LogEvent`, so the generation table does not hold the decoded event while
  the batch waits.
  """

  alias Logflare.Backends.IngestEventQueue.LogEventPointer

  @enforce_keys [:pointer, :json]
  defstruct [:pointer, :json]

  @type t :: %__MODULE__{
          pointer: LogEventPointer.t(),
          json: binary()
        }
end
