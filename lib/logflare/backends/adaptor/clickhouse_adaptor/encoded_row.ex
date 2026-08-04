defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.EncodedRow do
  @moduledoc """
  A ClickHouse RowBinary row and the queue pointer that owns it.

  The processor replaces the full `LogEvent` in the generation store with this
  representation after fused mapping and encoding. The large row binary is shared
  between ETS and Broadway messages by reference, and retries update and reinsert
  only the lightweight pointer without mapping or encoding the event again.
  """

  alias Logflare.Backends.IngestEventQueue.LogEventPointer

  @enforce_keys [:pointer, :row]
  defstruct [:pointer, :row]

  @type t :: %__MODULE__{
          pointer: LogEventPointer.t(),
          row: binary()
        }
end
