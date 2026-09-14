defmodule Logflare.Backends.Spool.Buffer do
  @moduledoc """
  Behaviour for how a `Partition` buffers appended segments and turns an
  accumulated batch into one commit — local-disk WAL (`Buffer.WAL`) or an
  in-memory batch (`Buffer.Mem`). A buffer owns how appends land and how
  rolling works; `Partition` owns reply timing, roll scheduling, and the
  commit task's lifecycle.
  """

  @type state :: term()
  @type body_thunk :: (-> {:ok, binary()} | {:error, term()})

  @doc "Builds this buffer's initial state from the same opts a Partition starts with."
  @callback init(opts :: keyword()) :: state()

  @doc """
  Appends one already-framed segment to the buffer. `:pending` means
  written but not yet durable — the caller is released by a later `:ok`
  append or the next roll.
  """
  @callback append(
              state(),
              segment :: binary(),
              raw_byte_size :: non_neg_integer(),
              event_count :: non_neg_integer()
            ) :: {:ok, state()} | {:pending, state()} | {:error, reason :: term(), state()}

  @doc """
  Seals whatever's accumulated into one commit, if there's anything to
  seal and (`force` or the buffer's own threshold is crossed). `context`
  is opaque, round-tripped back to `on_commit_result/3` once it settles.
  """
  @callback roll(state(), force :: boolean()) ::
              {:ok, body_thunk(), context :: term(), total_count :: non_neg_integer(), state()}
              | {:error, reason :: term(), state()}
              | {:no_roll, state()}

  @doc "Called once a roll's commit settles, so the buffer can do its own bookkeeping (e.g. delete a sealed WAL file)."
  @callback on_commit_result(state(), context :: term(), :ok | {:error, term()}) :: state()

  @doc "Finds any already-rolled, not-yet-committed work left behind by a crash."
  @callback recover(state()) ::
              {[{body_thunk(), context :: term(), total_count :: non_neg_integer()}], state()}
end
