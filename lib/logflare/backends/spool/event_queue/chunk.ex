defmodule Logflare.Backends.Spool.EventQueue.Chunk do
  @moduledoc """
  One caller's whole batch of events, claimed and acked as a single atomic
  unit by the spool producer — see `Logflare.Backends.Spool.EventQueue`.
  """

  alias Logflare.LogEvent

  @enforce_keys [:ref, :caller_pid, :events, :byte_size, :retries]
  defstruct [:ref, :caller_pid, :events, :byte_size, :retries]

  @type t :: %__MODULE__{
          ref: reference(),
          caller_pid: pid() | nil,
          events: [LogEvent.t()],
          byte_size: non_neg_integer(),
          retries: non_neg_integer()
        }
end
