defmodule Logflare.Backends.Spool.EventQueue.Chunk do
  @moduledoc """
  A lightweight pointer to one caller's whole batch of events, claimed and
  acked as a single atomic unit by the spool producer.

  Deliberately carries no event bodies — same reasoning as
  `Logflare.Backends.IngestEventQueue.LogEventPointer`: this struct is what
  travels through every Broadway stage hop (producer, processors, batcher,
  batch processors), each a real inter-process copy, so keeping it to a
  handful of small fields avoids copying potentially large event bodies at
  every hop. `byte_size`/`event_count` are precomputed at push time so batch
  splitting never needs to resolve the body either — only
  `Spool.ProducerPipeline.handle_batch/4` does, via
  `Logflare.Backends.Spool.EventQueue.get_events/1`, exactly once, right
  before encoding.
  """

  @enforce_keys [:ref, :caller_pid, :byte_size, :event_count, :retries]
  defstruct [:ref, :caller_pid, :byte_size, :event_count, :retries]

  @type t :: %__MODULE__{
          ref: reference(),
          caller_pid: pid() | nil,
          byte_size: non_neg_integer(),
          event_count: non_neg_integer(),
          retries: non_neg_integer()
        }
end
