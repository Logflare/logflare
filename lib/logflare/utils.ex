defmodule Logflare.Utils do
  @moduledoc """
  Context-only utilities. Should not be used outside of `lib/logflare/*`.
  """
  import Cachex.Spec
  import Logflare.Utils.Guards, only: [is_atom_value: 1]

  def cache_stats do
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
  def stringify_keys(%{} = map) do
    Map.new(map, fn
      {k, v} when is_atom(k) -> {Atom.to_string(k), stringify_keys(v)}
      {k, v} when is_binary(k) -> {k, stringify_keys(v)}
    end)
  end

  def stringify_keys([head | rest]) do
    [stringify_keys(head) | stringify_keys(rest)]
  end

  def stringify_keys(not_a_map), do: not_a_map

  @doc """
  Stringifies a term.

  ### Example
    iex> stringify(:my_atom)
    "my_atom"
    iex> stringify(1.1)
    "1.1"
    iex> stringify(122)
    "122"
    iex> stringify("something")
    "something"
    iex> stringify(%{})
    "%{}"
    iex> stringify([])
    "[]"
  """
  def stringify(v) when is_atom_value(v), do: Atom.to_string(v)
  def stringify(v) when is_binary(v), do: v
  def stringify(v) when is_float(v), do: Float.to_string(v)
  def stringify(v) when is_integer(v), do: Integer.to_string(v)
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

  @doc """
  Converts a Unix timestamp in microseconds to an ISO8601 string.

  ## Examples

    iex> iso_timestamp(1609459200000000)
    "2021-01-01T00:00:00Z"

    iex> iso_timestamp(:not_a_timestamp)
    nil
  """
  def iso_timestamp(timestamp) when is_integer(timestamp) do
    timestamp
    |> DateTime.from_unix!(:microsecond)
    |> DateTime.truncate(:second)
    |> DateTime.to_iso8601()
  end

  def iso_timestamp(_timestamp), do: nil

  @doc """
  Determines the IP version of an address.

  iex> ip_version("127.0.0.1")
  :inet

  iex> ip_version("127.0.0.1:8222")
  nil

  iex> ip_version("1467:f4e1:7a77:756a:896c:dff5:ca48:cf3c")
  :inet6

  iex> ip_version("not_an_address")
  nil
  """
  @spec ip_version(String.t()) :: :inet | :inet6 | nil
  def ip_version(address) when is_binary(address) do
    case :inet.parse_address(String.to_charlist(address)) do
      {:ok, {_, _, _, _}} -> :inet
      {:ok, {_, _, _, _, _, _, _, _}} -> :inet6
      {:error, _} -> nil
    end
  end
end
