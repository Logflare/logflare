defmodule Logflare.Rules.RoutingSnapshot do
  @moduledoc """
  Immutable rule storage for one routing-tree generation.

  The hot cache header contains binary/table references rather than the full
  rule map. A sorted binary index maps rule IDs to ETS tuple positions. Sparse
  reads copy only the matched tuple elements; dense reads copy the whole tuple.
  Every lookup uses the exact generation in the header.

  The compressed map is a reader-owned fallback, not the normal lookup path. If
  the backing store replaces, evicts or loses a generation, the reader decodes
  its own immutable snapshot instead. The VM keeps this reference-counted binary
  alive across cache invalidation and releases it on GC/process exit. Thus even
  a suspended reader needs no lease, cleanup timer or retained ETS generation.

  The tree and map must be constructed from the same database read before this
  header is published. Encodings are internal, never user-supplied binaries.
  """

  alias Logflare.Rules.Rule
  alias Logflare.Rules.RoutingSnapshotStore

  @entry_bytes 8

  @enforce_keys [:key, :table, :index, :encoded, :count]
  defstruct [:key, :table, :index, :encoded, :count]

  @type t() :: %__MODULE__{
          key: {integer(), reference()},
          table: :ets.tid(),
          index: binary(),
          encoded: binary(),
          count: non_neg_integer()
        }

  @spec new(integer(), %{Rule.id() => Rule.t()}, GenServer.server()) :: t()
  def new(source_id, rules_by_id, store \\ RoutingSnapshotStore) do
    entries = Enum.sort_by(rules_by_id, &elem(&1, 0))
    index = for {id, _rule} <- entries, into: <<>>, do: <<id::unsigned-64>>
    encoded = :erlang.term_to_binary(rules_by_id, compressed: 1)
    {table, key} = RoutingSnapshotStore.put(store, source_id, entries)

    %__MODULE__{
      key: key,
      table: table,
      index: index,
      encoded: encoded,
      count: map_size(rules_by_id)
    }
  end

  @spec resolve(t(), [Rule.id()]) :: [Rule.t()]
  def resolve(_snapshot, []), do: []

  def resolve(%__MODULE__{count: count} = snapshot, ids) when length(ids) * 2 >= count do
    case read_all(snapshot) do
      [entries] -> entries |> Tuple.to_list() |> tl() |> Map.new() |> from_map(ids)
      [] -> snapshot.encoded |> :erlang.binary_to_term() |> from_map(ids)
    end
  end

  def resolve(%__MODULE__{} = snapshot, ids) do
    case read_sparse(snapshot, ids, []) do
      {:ok, rules} -> rules
      :missing -> snapshot.encoded |> :erlang.binary_to_term() |> from_map(ids)
    end
  end

  defp read_sparse(_snapshot, [], acc), do: {:ok, Enum.reverse(acc)}

  defp read_sparse(snapshot, [id | rest], acc) do
    case position(snapshot.index, id, 0, snapshot.count - 1) do
      nil ->
        read_sparse(snapshot, rest, acc)

      position ->
        case read_element(snapshot, position + 2) do
          {_id, nil} -> read_sparse(snapshot, rest, acc)
          {_id, rule} -> read_sparse(snapshot, rest, [rule | acc])
          :missing -> :missing
        end
    end
  end

  defp read_all(snapshot) do
    :ets.lookup(snapshot.table, snapshot.key)
  rescue
    ArgumentError -> []
  end

  defp read_element(snapshot, position) do
    :ets.lookup_element(snapshot.table, snapshot.key, position)
  rescue
    ArgumentError -> :missing
  end

  defp position(_index, _id, low, high) when low > high, do: nil

  defp position(index, id, low, high) do
    middle = div(low + high, 2)
    <<key::unsigned-64>> = binary_part(index, middle * @entry_bytes, @entry_bytes)

    cond do
      id < key -> position(index, id, low, middle - 1)
      id > key -> position(index, id, middle + 1, high)
      true -> middle
    end
  end

  defp from_map(rules_by_id, ids) do
    for id <- ids, rule = Map.get(rules_by_id, id), do: rule
  end
end
