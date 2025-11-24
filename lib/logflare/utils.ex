defmodule Logflare.Utils do
  @moduledoc """
  Context-only utilities. Should not be used outside of `lib/logflare/*`.
  """
  import Cachex.Spec
  import Logflare.Utils.Guards, only: [is_atom_value: 1]

  @original_to_string String.Chars.impl_for(%Tesla.Env{})
  def tesla_env_to_string(), do: @original_to_string

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
  def stringify_keys(%{} = map) do
    for {k, v} <- map, into: %{} do
      {to_string_key(k), deep_stringify(v)}
    end
  end

  defp deep_stringify(%{} = map), do: stringify_keys(map)
  defp deep_stringify([_ | _] = list), do: Enum.map(list, &deep_stringify/1)
  defp deep_stringify(v), do: v

  defp to_string_key(k) when is_atom(k), do: Atom.to_string(k)
  defp to_string_key(k) when is_binary(k), do: k

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
  Appends a value to the end of a tuple.

  ## Examples

    iex> Logflare.Utils.append_to_tuple({:a, :b}, :c)
    {:a, :b, :c}

    iex> Logflare.Utils.append_to_tuple({}, :a)
    {:a}
  """
  def append_to_tuple(tuple, value) do
    Tuple.insert_at(tuple, tuple_size(tuple), value)
  end

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

  iex> ip_version(nil)
  nil
  """
  @spec ip_version(String.t() | nil) :: :inet | :inet6 | nil
  def ip_version(address) when is_binary(address) do
    case :inet.parse_address(String.to_charlist(address)) do
      {:ok, {_, _, _, _}} -> :inet
      {:ok, {_, _, _, _, _, _, _, _}} -> :inet6
      {:error, _} -> nil
    end
  end

  def ip_version(_address), do: nil

  @doc """
  Redacts sensitive headers from a list of Tesla.Env headers. Used for automatic redaction.
  """
  @spec redact_sensitive_headers(map()) :: list(tuple())
  def redact_sensitive_headers(%{} = value) do
    value
    |> Iteraptor.map(
      fn
        {[key | _], value}
        when is_binary(value) and
               key in ["authorization", "x-api-key", "Authorization", "X-API-Key"] ->
          "REDACTED"

        self ->
          self
      end,
      keys: :reverse,
      structs: :keep
    )
  end

  @doc """
  Receives the previous inspect function and performs redaction if it is a Tesla.Env.
  Does nothing if it is not a Tesla.Env or Tesla.Client.
  """
  def inspect_fun(prev_fun, value, opts)
      when is_struct(value, Tesla.Env) or is_struct(value, Tesla.Client) do
    if Application.get_env(:logflare, :env) in [:test, :dev] do
      prev_fun.(value, opts)
    else
      value = Logflare.Utils.redact_sensitive_headers(value)
      prev_fun.(value, opts)
    end
  end

  def inspect_fun(prev_fun, value, opts) do
    prev_fun.(value, opts)
  end

  # helper function for custom String.Chars defimpl
  @doc false
  def stringify_tesla_struct(struct) do
    if Application.get_env(:logflare, :env) in [:test, :dev] do
      apply(Logflare.Utils.tesla_env_to_string(), :to_string, [struct])
    else
      value = Logflare.Utils.redact_sensitive_headers(struct)

      apply(Logflare.Utils.tesla_env_to_string(), :to_string, [value])
    end
  end

  defimpl String.Chars, for: Tesla.Client do
    def to_string(%{} = env) do
      Logflare.Utils.stringify_tesla_struct(env)
    end
  end

  defimpl String.Chars, for: Tesla.Env do
    def to_string(%{} = env) do
      Logflare.Utils.stringify_tesla_struct(env)
    end
  end

  @doc """
  Returns information about the ets table as a list of tuples.
  This is just a wrapper of :ets.info/1 to enable mocking without having issues
  from mocking the :ets module
  """
  def ets_info(table), do: :ets.info(table)

  @doc """
  Tries to stop a process gracefully. If it fails, it sends a signal to the process.
  """
  @spec try_to_stop_process(pid(), atom()) :: :ok | :noop
  def try_to_stop_process(pid, signal \\ :shutdown, force_signal \\ :kill) do
    GenServer.stop(pid, signal, 5_000)
    :ok
  rescue
    _ ->
      Process.exit(pid, force_signal)
      :ok
  catch
    :exit, _ ->
      :noop
  end
end
