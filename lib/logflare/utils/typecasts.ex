defmodule Logflare.Typecasts do
  @moduledoc false
  def maybe_string_to_integer_or_zero(nil), do: 0
  def maybe_string_to_integer_or_zero(s) when is_integer(s), do: s
  def maybe_string_to_integer_or_zero(s) when is_binary(s), do: String.to_integer(s)
end
