defmodule Logflare.Backends.IngestEventQueue.LogEventPointer do
  @moduledoc """
  A lightweight pointer to a claimed event, returned by
  `Logflare.Backends.IngestEventQueue.pop_pending_pointers/2`.

  `id` is the event's own id — kept as the pointer table's key so a duplicate insert of
  the same id into the same queue is naturally rejected rather than silently claiming a
  second pointer for it (see `Logflare.Backends.IngestEventQueue.insert_pointer_batch/3`).
  `gen_event_id` is the *actual* key the event body lives under in the generation table
  (`tid`) — a fresh, unique reference generated per insert rather than reusing `id`, so
  independent producers that happen to receive duplicate copies of the same event id
  never collide on the same generation-store row. Resolving the event body is a direct
  `:ets.lookup(tid, gen_event_id)`. `queue_tid` is the pending-queue table this pointer
  was claimed from, carried only so a retry can reinsert directly into it (see
  `Logflare.Backends.IngestEventQueue.reinsert_pointer/1`) without going through
  round-robin redistribution — the producer that owns `queue_tid` just proved itself
  alive by claiming this.
  """

  alias Logflare.LogEvent.TypeDetection

  @enforce_keys [
    :id,
    :tid,
    :gen_event_id,
    :queue_tid,
    :size,
    :retries,
    :event_type,
    :day_bucket
  ]
  defstruct [
    :id,
    :tid,
    :gen_event_id,
    :queue_tid,
    :size,
    :retries,
    :event_type,
    :day_bucket
  ]

  @type t :: %__MODULE__{
          id: term(),
          tid: :ets.tid(),
          gen_event_id: reference(),
          queue_tid: :ets.tid(),
          size: non_neg_integer(),
          retries: non_neg_integer(),
          event_type: TypeDetection.event_type(),
          day_bucket: integer()
        }
end
