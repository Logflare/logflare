defmodule Logflare.Backends.Spool.Buffer do
  @moduledoc """
  Behaviour for how a `Logflare.Backends.Spool.Partition` physically
  buffers appended segments and turns an accumulated batch into one
  commit — local-disk WAL (`Buffer.WAL`) or an in-memory batch
  (`Buffer.Mem`). A buffer owns *how* appends land and *how* rolling
  works, including reporting its own local write/roll failures (e.g.
  `Logflare.Backends.Spool.Health` for the WAL buffer; a no-op for Mem,
  since there's no local disk to report on) — commit-result health
  reporting itself is handled uniformly for every buffer by `Partition`,
  not here (see `on_commit_result/3` below).

  `Partition` owns everything else: when to reply to a caller
  (immediately, by default, vs deferred until the eventual commit settles,
  for `append/5` with `wait_until_committed: true`), when to attempt a roll (after every
  append, and on a recurring timer), and the commit `Task`'s lifecycle
  (concurrency, retries — via `Logflare.Backends.Spool.Committer` — and
  crash handling). A buffer never sees `Committer` or any caller's `from`
  at all — it only ever hands `Partition` a way to get the bytes
  (`body_thunk`) and an opaque `context` it wants back later.
  """

  @type state :: term()
  @type body_thunk :: (-> {:ok, binary()} | {:error, term()})

  @doc "Builds this buffer's initial state from the same opts a Partition starts with."
  @callback init(opts :: keyword()) :: state()

  @doc "Appends one already-framed segment to the buffer."
  @callback append(
              state(),
              segment :: binary(),
              raw_byte_size :: non_neg_integer(),
              event_count :: non_neg_integer()
            ) :: {:ok, state()} | {:error, reason :: term(), state()}

  @doc """
  Seals whatever's accumulated into one commit, if there's anything to
  seal and (`force` or the buffer's own threshold is crossed) — `force` is
  true on every recurring timer tick, so however little has accumulated
  still gets committed eventually rather than waiting indefinitely for a
  threshold that may never come. `context` is opaque to `Partition` —
  round-tripped back to `on_commit_result/3` unchanged once the commit
  this roll started settles.
  """
  @callback roll(state(), force :: boolean()) ::
              {:ok, body_thunk(), context :: term(), total_count :: non_neg_integer(), state()}
              | {:error, reason :: term(), state()}
              | {:no_roll, state()}

  @doc """
  Called once a roll's commit settles, successfully or not, so the buffer
  can do its own bookkeeping (e.g. delete a sealed WAL file on success) —
  a no-op for a buffer with nothing of its own to do here (e.g. Mem).
  """
  @callback on_commit_result(state(), context :: term(), :ok | {:error, term()}) :: state()

  @doc """
  Finds any already-rolled, not-yet-committed work left behind by a crash
  — always `[]` for a buffer with nothing durable to recover (e.g. Mem).
  """
  @callback recover(state()) ::
              {[{body_thunk(), context :: term(), total_count :: non_neg_integer()}], state()}
end
