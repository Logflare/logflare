defmodule Logflare.Utils do
  @moduledoc """
  Context-only utilities. Should not be used outside of `lib/logflare/*`.
  """
  import Cachex.Spec

  def cache_stats() do
    hook(module: Cachex.Stats)
  end

  def cache_limit(n) when is_integer(n) do
    hook(
      module: Cachex.Limit.Scheduled,
      args: {
        # setting cache max size
        n,
        # options for `Cachex.prune/3`
        [],
        # options for `Cachex.Limit.Scheduled`
        []
      }
    )
  end

  @doc """
  Builds a long Cachex expiration spec
  Defaults to 20 min with 5 min cleanup intervals
  """
  @spec cache_expiration_min(non_neg_integer(), non_neg_integer()) :: Cachex.Spec.expiration()
  def cache_expiration_min(default \\ 20, interval \\ 5) do
    cache_expiration_sec(default * 60, interval * 60)
  end

  @doc """
  Builds a short Cachex expiration spec
  Defaults to 50 sec with 20 sec cleanup intervals
  """
  @spec cache_expiration_sec(non_neg_integer(), non_neg_integer()) :: Cachex.Spec.expiration()
  def cache_expiration_sec(default \\ 60, interval \\ 20) do
    expiration(
      # default record expiration of 20 mins
      default: :timer.seconds(default),
      # how often cleanup should occur, 5 mins
      interval: :timer.seconds(interval),
      # whether to enable lazy checking
      lazy: true
    )
  end

  @doc """
  Stringifies an atom map to a string map.

  ## Examples

    iex> stringify_keys(%{test: "data"})
    %{"test" => "data"}
  """
  @spec stringify_keys(map()) :: map()
  def stringify_keys(map = %{}) do
    Map.new(map, fn
      {k, v} when is_atom(k) -> {Atom.to_string(k), stringify_keys(v)}
      {k, v} when is_binary(k) -> {k, stringify_keys(v)}
    end)
  end

  def stringify_keys([head | rest]) do
    [stringify_keys(head) | stringify_keys(rest)]
  end

  def stringify_keys(not_a_map), do: not_a_map

  def stringify(v) when is_integer(v) do
    Integer.to_string(v)
  end

  def stringify(v) when is_float(v) do
    Float.to_string(v)
  end

  def stringify(v) when is_binary(v), do: v
  def stringify(v), do: inspect(v)

  @doc """
  Sets the default ecto changeset field value if not set

  ## Examples

    iex> data = %{title: "hello"}
    iex> types = %{title: :string}
    iex> changeset = Ecto.Changeset.cast({data, types}, %{title: nil}, [:title])
    iex> %Ecto.Changeset{changes: %{title: "123"}} =  default_field_value(changeset, :title, "123")
  """
  def default_field_value(%Ecto.Changeset{} = changeset, field, value) do
    if Ecto.Changeset.get_field(changeset, field) !== nil do
      changeset
    else
      Ecto.Changeset.put_change(changeset, field, value)
    end
  end

  @doc """
  Performs chunked round robin of a batch of items to a group of targets.
  Repeats the group of targets until the batch is completely empty.

  ## Examples

    iex> batch = [1, 2, 4]
    iex> targets = [:x, :y]
    iex> chunk_size = 2
    iex> chunked_round_robin(batch, targets, chunk_size, fn chunk, target -> {target, Enum.sum(chunk)} end )
    [x: 3, y: 4]


    iex> batch = [1, 1, 2, 2, 3, 3, 4, 4]
    iex> targets = [:x]
    iex> chunk_size = 2
    iex> chunked_round_robin(batch, targets, chunk_size, fn chunk, target -> {target, Enum.sum(chunk)} end )
    [x: 20]
  """
  def chunked_round_robin(batch, [target], chunk_size, func) do
    result = func.(batch, target)

    next_chunk_rr(
      [],
      chunk_size,
      [],
      [target],
      func,
      [result]
    )
  end

  def chunked_round_robin(batch, targets, chunk_size, func) do
    next_chunk_rr(batch, chunk_size, targets, targets, func, [])
  end

  @doc """
  Takes an adapter config and encodes basic auth.
  """
  def encode_basic_auth(%{username: username, password: password})
      when is_binary(username) and is_binary(password) do
    Base.encode64(username <> ":" <> password)
  end

  def encode_basic_auth(_adapter_config), do: nil

  defp next_chunk_rr([], _chunk_size, _remaining, _initial_targets, _func, results) do
    Enum.reverse(results)
  end

  defp next_chunk_rr(batch, chunk_size, [], initial_targets, func, results) do
    next_chunk_rr(batch, chunk_size, initial_targets, initial_targets, func, results)
  end

  defp next_chunk_rr(
         batch,
         chunk_size,
         [target | remaining_targets],
         initial_targets,
         func,
         results
       ) do
    {chunk, remainder} = Enum.split(batch, chunk_size)
    result = func.(chunk, target)

    next_chunk_rr(
      remainder,
      chunk_size,
      remaining_targets,
      initial_targets,
      func,
      [result | results]
    )
  end
end
