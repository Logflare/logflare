defmodule Logflare.Backends.IngestEventQueue.LogEventPointer do
  @moduledoc """
  A lightweight pointer to a claimed event, returned by
  `Logflare.Backends.IngestEventQueue.take_pending_pointers/2`.

  `tid` is the generation table the actual `LogEvent` body lives in — resolving it is a
  direct `:ets.lookup(tid, id)`, no separate id-to-table lookup needed. `queue_tid` is
  the pending-queue table this pointer was claimed from, carried only so a retry can
  reinsert directly into it (see `Logflare.Backends.IngestEventQueue.reinsert_pointer/1`)
  without going through round-robin redistribution — the producer that owns `queue_tid`
  just proved itself alive by claiming this.
  """

  alias Logflare.LogEvent.TypeDetection

  @enforce_keys [
    :id,
    :tid,
    :queue_tid,
    :size,
    :retries,
    :event_type,
    :day_bucket,
    :ingest_freshness
  ]
  defstruct [
    :id,
    :tid,
    :queue_tid,
    :size,
    :retries,
    :event_type,
    :day_bucket,
    :ingest_freshness
  ]

  @type t :: %__MODULE__{
          id: term(),
          tid: :ets.tid(),
          queue_tid: :ets.tid(),
          size: non_neg_integer(),
          retries: non_neg_integer(),
          event_type: TypeDetection.event_type(),
          day_bucket: integer(),
          ingest_freshness: :fresh | :stale
        }
end
