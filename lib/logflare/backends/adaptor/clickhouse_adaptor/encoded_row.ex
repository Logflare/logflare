defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.EncodedRow do
  @moduledoc """
  A ClickHouse RowBinary row and the queue pointer that owns it.

  After fused mapping and encoding, the processor stores this representation in the
  generation table while that table remains live. The Broadway message independently
  retains the row across concurrent generation eviction. Normally the large binary is
  shared by reference between ETS and the message, and retries update only the pointer
  without mapping or encoding the event again.
  """

  alias Logflare.Backends.IngestEventQueue.LogEventPointer

  @enforce_keys [:pointer, :row]
  defstruct [:pointer, :row]

  @type t :: %__MODULE__{
          pointer: LogEventPointer.t(),
          row: binary()
        }
end
