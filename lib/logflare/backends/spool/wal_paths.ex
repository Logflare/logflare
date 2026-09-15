defmodule Logflare.Backends.Spool.WalPaths do
  @moduledoc """
  Shared local WAL file naming for a partition index: `p<index>.wal` for
  the active file, `p<index>-<seq>.sealed` for a rolled/rotated one.
  """

  @spec active(Path.t(), non_neg_integer()) :: Path.t()
  def active(wal_dir, index), do: Path.join(wal_dir, "p#{index}.wal")

  @spec sealed(Path.t(), non_neg_integer()) :: Path.t()
  def sealed(wal_dir, index) do
    seq = :erlang.unique_integer([:positive, :monotonic])
    Path.join(wal_dir, "p#{index}-#{seq}.sealed")
  end

  @doc "Glob pattern matching every sealed file left behind for partition `index`."
  @spec sealed_glob(Path.t(), non_neg_integer()) :: Path.t()
  def sealed_glob(wal_dir, index), do: Path.join(wal_dir, "p#{index}-*.sealed")
end
