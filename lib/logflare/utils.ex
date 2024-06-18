defmodule Logflare.Utils do
  @moduledoc """
  Context-only utilities. Should not be used outside of `lib/logflare/*`
  """
  import Cachex.Spec

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

  ### Example
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
  def stringify(v) when is_atom(v), do: Atom.to_string(v)
  def stringify(v) when is_binary(v), do: v
  def stringify(v) when is_float(v), do: Float.to_string(v)
  def stringify(v) when is_integer(v), do: Integer.to_string(v)
  def stringify(v), do: inspect(v)

  @doc """
  Sets the default ecto changeset field value if not set

  ###  Example
    iex> data = %{title: "hello"}
    iex> types = %{title: :string}
    iex> changeset = Ecto.Changeset.cast({data, types}, %{title: nil}, [:title])
    iex> %Ecto.Changeset{changes: %{title: "123"}} =  default_field_value(changeset, :title, "123")
  """
  def default_field_value(%Ecto.Changeset{} = changeset, field, value) do
    if Ecto.Changeset.get_field(changeset, field) do
      changeset
    else
      Ecto.Changeset.put_change(changeset, field, value)
    end
  end
end
