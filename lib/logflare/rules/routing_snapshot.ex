defmodule Logflare.Rules.RoutingSnapshot do
  @moduledoc """
  Immutable compact routing targets for one positional rules-tree generation.

  The tree emits zero-based tuple positions rather than database IDs. Sparse
  reads copy only those ETS tuple elements, while dense reads copy the tuple once
  and address it directly. No per-event ID index or full-map reconstruction is
  required.

  The compressed target tuple is a reader-owned fallback. If the backing store
  replaces, evicts or loses a generation, the caller resolves from that exact
  immutable tuple. Restore only repopulates disposable ETS acceleration; the header
  and its generation never change.
  """

  alias Logflare.Rules.RoutingSnapshotStore
  alias Logflare.Rules.Rule
  alias Logflare.Sources.SourceRouter.Target

  @enforce_keys [:key, :table, :encoded, :count, :estimated_bytes]
  defstruct [:key, :table, :encoded, :count, :estimated_bytes, decoded: nil]

  @type t() :: %__MODULE__{
          key: RoutingSnapshotStore.key(),
          table: RoutingSnapshotStore.table(),
          encoded: binary(),
          count: non_neg_integer(),
          estimated_bytes: non_neg_integer(),
          decoded: tuple() | nil
        }

  @type resolve_status() ::
          {:ok, [Target.t()]} | {:fallback, [Target.t()], tuple()}

  @spec new(integer(), [Target.t()], keyword()) :: t()
  def new(source_id, targets, opts \\ []) when is_list(targets) do
    target_tuple = List.to_tuple(targets)
    encoded = :erlang.term_to_binary(target_tuple, compressed: 1)

    estimated_bytes =
      :erlang.external_size(target_tuple) + byte_size(encoded) +
        Keyword.get(opts, :extra_estimated_bytes, 0)

    store = Keyword.get(opts, :store, RoutingSnapshotStore)

    key = {source_id, :erlang.unique_integer([:monotonic, :positive])}
    table = put_or_fallback(store, key, target_tuple, estimated_bytes, opts)

    %__MODULE__{
      key: key,
      table: table,
      encoded: encoded,
      count: tuple_size(target_tuple),
      estimated_bytes: estimated_bytes
    }
  end

  @spec put_or_fallback(
          GenServer.server(),
          RoutingSnapshotStore.key(),
          tuple(),
          non_neg_integer(),
          keyword()
        ) :: RoutingSnapshotStore.table()
  defp put_or_fallback(store, key, entries, estimated_bytes, opts) do
    RoutingSnapshotStore.put(store, key, entries, estimated_bytes)
  catch
    :exit, _reason -> Keyword.get(opts, :table, RoutingSnapshotStore)
  end

  @doc false
  @spec restore(t(), GenServer.server()) :: :ok
  def restore(%__MODULE__{} = snapshot, store \\ RoutingSnapshotStore) do
    RoutingSnapshotStore.restore(
      store,
      snapshot.table,
      snapshot.key,
      snapshot.encoded,
      snapshot.estimated_bytes
    )
  end

  @doc false
  @spec contains_any_rule?(t(), MapSet.t(Rule.id())) :: boolean()
  def contains_any_rule?(%__MODULE__{} = snapshot, ids) do
    targets = :erlang.binary_to_term(snapshot.encoded, [:safe])
    contains_target_id?(targets, ids, tuple_size(targets))
  end

  @spec contains_target_id?(tuple(), MapSet.t(Rule.id()), non_neg_integer()) :: boolean()
  defp contains_target_id?(_targets, _ids, 0), do: false

  defp contains_target_id?(targets, ids, remaining) do
    case elem(targets, remaining - 1) do
      {id, _backend_id, _sink} ->
        MapSet.member?(ids, id) or contains_target_id?(targets, ids, remaining - 1)

      nil ->
        contains_target_id?(targets, ids, remaining - 1)
    end
  end

  @spec resolve(t(), [non_neg_integer()]) :: [Target.t()]
  def resolve(snapshot, positions) do
    case resolve_with_status(snapshot, positions) do
      {:ok, targets} -> targets
      {:fallback, targets, _encoded_targets} -> targets
    end
  end

  @doc false
  @spec resolve_with_status(t(), [non_neg_integer()]) :: resolve_status()
  def resolve_with_status(_snapshot, []), do: {:ok, []}

  def resolve_with_status(%__MODULE__{decoded: targets}, positions) when is_tuple(targets),
    do: {:ok, from_target_tuple(targets, positions)}

  def resolve_with_status(%__MODULE__{count: count} = snapshot, positions)
      when length(positions) * 2 >= count do
    case read_all(snapshot) do
      [entries] -> {:ok, from_store_tuple(entries, positions, count)}
      [] -> from_fallback(snapshot, positions)
    end
  end

  def resolve_with_status(%__MODULE__{} = snapshot, positions) do
    case read_sparse(snapshot, positions, []) do
      {:ok, targets} -> {:ok, targets}
      :missing -> from_fallback(snapshot, positions)
    end
  end

  defp read_sparse(_snapshot, [], acc), do: {:ok, Enum.reverse(acc)}

  defp read_sparse(%__MODULE__{count: count} = snapshot, [position | rest], acc) do
    if valid_position?(position, count) do
      case read_element(snapshot, position + 2) do
        nil -> read_sparse(snapshot, rest, acc)
        :missing -> :missing
        target -> read_sparse(snapshot, rest, [target | acc])
      end
    else
      read_sparse(snapshot, rest, acc)
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

  defp from_store_tuple(entries, positions, count) do
    for position <- positions,
        valid_position?(position, count),
        target = elem(entries, position + 1),
        target != nil,
        do: target
  end

  defp from_fallback(snapshot, positions) do
    targets = :erlang.binary_to_term(snapshot.encoded)
    {:fallback, from_target_tuple(targets, positions), targets}
  end

  @doc false
  @spec with_decoded(t(), tuple()) :: t()
  def with_decoded(%__MODULE__{} = snapshot, targets) when is_tuple(targets),
    do: %{snapshot | decoded: targets}

  defp from_target_tuple(targets, positions) do
    count = tuple_size(targets)

    for position <- positions,
        valid_position?(position, count),
        target = elem(targets, position),
        target != nil,
        do: target
  end

  defp valid_position?(position, count),
    do: is_integer(position) and position >= 0 and position < count
end
