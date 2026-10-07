defmodule Logflare.Backends.Spool.WalPaths do
  @moduledoc """
  Shared local WAL file naming for a partition index: `p<index>.wal` for
  the active file, `p<index>-<seq>.sealed` for a rolled/rotated one.
  """

  @spec active(Path.t(), non_neg_integer()) :: Path.t()
  def active(wal_dir, index), do: Path.join(wal_dir, "p#{index}.wal")

  @spec sealed(Path.t(), non_neg_integer()) :: Path.t()
  def sealed(wal_dir, index) do
    # :erlang.unique_integer/1 is only unique within the current runtime
    # instance — its counter resets on every restart, so a leftover sealed
    # file from before a crash could collide with a freshly-generated name
    # and get silently clobbered by File.rename/2. system_time doesn't
    # repeat across restarts; unique_integer still guards against two
    # rotations landing in the same process within the same nanosecond.
    seq = :erlang.unique_integer([:positive, :monotonic])
    system_time = System.system_time(:nanosecond)
    Path.join(wal_dir, "p#{index}-#{system_time}-#{seq}.sealed")
  end

  @doc "Glob pattern matching every sealed file left behind for partition `index`."
  @spec sealed_glob(Path.t(), non_neg_integer()) :: Path.t()
  def sealed_glob(wal_dir, index), do: Path.join(wal_dir, "p#{index}-*.sealed")
end
