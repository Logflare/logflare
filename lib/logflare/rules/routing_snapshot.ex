defmodule Logflare.Rules.RoutingSnapshot do
  @moduledoc """
  Immutable compact routing targets for one rules-tree generation.

  The tree emits database rule IDs. A sorted binary index maps those IDs to ETS
  tuple positions, so sparse reads copy only matched compact targets. Dense reads
  copy the tuple once and reconstruct the compact lookup map.

  The compressed target map is a reader-owned fallback. If the backing store
  replaces, evicts or loses a generation, the caller resolves from that exact
  immutable map and can conditionally rehydrate the still-current cache entry.
  """

  alias Logflare.Rules.Rule
  alias Logflare.Rules.RoutingSnapshotStore
  alias Logflare.Sources.SourceRouter.Target

  @entry_bytes 8

  @enforce_keys [:key, :table, :index, :encoded, :count, :estimated_bytes]
  defstruct [:key, :table, :index, :encoded, :count, :estimated_bytes, decoded: nil]

  @type t() :: %__MODULE__{
          key: {integer(), reference()},
          table: :ets.tid() | nil,
          index: binary(),
          encoded: binary(),
          count: non_neg_integer(),
          estimated_bytes: non_neg_integer(),
          decoded: %{Rule.id() => Target.t()} | nil
        }

  @type resolve_status() ::
          {:ok, [Target.t()]} | {:fallback, [Target.t()], %{Rule.id() => Target.t()}}

  @spec new(integer(), [{Rule.id(), Target.t()}], keyword()) :: t()
  def new(source_id, entries, opts \\ []) when is_list(entries) do
    entries = Enum.sort_by(entries, &elem(&1, 0))
    rules_by_id = Map.new(entries)
    entry_tuple = List.to_tuple(entries)
    index = for {id, _target} <- entries, into: <<>>, do: <<id::unsigned-64>>
    encoded = :erlang.term_to_binary(rules_by_id, compressed: 1)

    estimated_bytes =
      :erlang.external_size(entry_tuple) + byte_size(index) + byte_size(encoded) +
        Keyword.get(opts, :extra_estimated_bytes, 0)

    store = Keyword.get(opts, :store, RoutingSnapshotStore)
    {table, key} = put_or_fallback(store, source_id, entry_tuple, estimated_bytes)

    %__MODULE__{
      key: key,
      table: table,
      index: index,
      encoded: encoded,
      count: length(entries),
      estimated_bytes: estimated_bytes
    }
  end

  defp put_or_fallback(store, source_id, entries, estimated_bytes) do
    RoutingSnapshotStore.put(store, source_id, entries, estimated_bytes)
  catch
    :exit, _reason -> {nil, {source_id, make_ref()}}
  end

  @doc false
  @spec rehydrate(t(), integer(), %{Rule.id() => Target.t()}, GenServer.server()) :: t()
  def rehydrate(
        %__MODULE__{} = snapshot,
        source_id,
        rules_by_id,
        store \\ RoutingSnapshotStore
      )
      when is_map(rules_by_id) do
    entries = rules_by_id |> Enum.sort_by(&elem(&1, 0)) |> List.to_tuple()

    {table, key} =
      RoutingSnapshotStore.put(store, source_id, entries, snapshot.estimated_bytes)

    %{snapshot | table: table, key: key, decoded: nil}
  end

  @spec resolve(t(), [Rule.id()]) :: [Target.t()]
  def resolve(snapshot, ids) do
    case resolve_with_status(snapshot, ids) do
      {:ok, targets} -> targets
      {:fallback, targets, _rules_by_id} -> targets
    end
  end

  @doc false
  @spec resolve_with_status(t(), [Rule.id()]) :: resolve_status()
  def resolve_with_status(_snapshot, []), do: {:ok, []}

  def resolve_with_status(%__MODULE__{decoded: rules_by_id}, ids) when is_map(rules_by_id),
    do: {:ok, from_map(rules_by_id, ids)}

  def resolve_with_status(%__MODULE__{count: count} = snapshot, ids)
      when length(ids) * 2 >= count do
    case read_all(snapshot) do
      [entries] -> {:ok, entries |> Tuple.to_list() |> tl() |> Map.new() |> from_map(ids)}
      [] -> from_fallback(snapshot, ids)
    end
  end

  def resolve_with_status(%__MODULE__{} = snapshot, ids) do
    case read_sparse(snapshot, ids, []) do
      {:ok, targets} -> {:ok, targets}
      :missing -> from_fallback(snapshot, ids)
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
          {_id, target} -> read_sparse(snapshot, rest, [target | acc])
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

  defp from_fallback(snapshot, ids) do
    rules_by_id = :erlang.binary_to_term(snapshot.encoded)
    {:fallback, from_map(rules_by_id, ids), rules_by_id}
  end

  @doc false
  @spec with_decoded(t(), %{Rule.id() => Target.t()}) :: t()
  def with_decoded(%__MODULE__{} = snapshot, rules_by_id) when is_map(rules_by_id),
    do: %{snapshot | decoded: rules_by_id}

  defp from_map(rules_by_id, ids) do
    for id <- ids, target = Map.get(rules_by_id, id), do: target
  end
end
