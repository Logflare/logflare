defmodule Logflare.LogEvent.Size do
  @moduledoc """
  Values-only ingestion accounting, independent of transport and storage encoding.

  Strings contribute their byte length, numbers their Elixir textual length,
  booleans one byte, and nil zero bytes. Maps exclude keys and lists exclude
  structural overhead. Non-JSON leaves retain Erlang external-format sizing.
  """

  @spec body_byte_size(term()) :: non_neg_integer()
  def body_byte_size(value) when is_binary(value), do: byte_size(value)
  def body_byte_size(value) when is_integer(value) and value < 0, do: 1 + integer_digits(-value)
  def body_byte_size(value) when is_integer(value), do: integer_digits(value)
  def body_byte_size(value) when is_float(value), do: byte_size(Float.to_string(value))
  def body_byte_size(value) when is_boolean(value), do: 1
  def body_byte_size(nil), do: 0
  def body_byte_size(value) when is_atom(value), do: byte_size(Atom.to_string(value))

  def body_byte_size(value) when is_map(value) and not is_struct(value) do
    :maps.fold(fn _key, value, bytes -> bytes + body_byte_size(value) end, 0, value)
  end

  def body_byte_size(value) when is_list(value) do
    Enum.reduce(value, 0, fn value, bytes -> bytes + body_byte_size(value) end)
  end

  def body_byte_size(value), do: :erlang.external_size(value)

  @spec integer_digits(non_neg_integer()) :: pos_integer()
  defp integer_digits(value) when value < 10, do: 1
  defp integer_digits(value) when value < 100, do: 2
  defp integer_digits(value) when value < 1000, do: 3
  defp integer_digits(value), do: 3 + integer_digits(div(value, 1000))
end
