defmodule Logflare.TestUtils do
  @moduledoc """
  Testing utilities. Globally alised under the `TestUtils` namespace.
  """

  @spec random_string(non_neg_integer()) :: String.t()
  def random_string(length \\ 6) do
    :crypto.strong_rand_bytes(length) |> Base.url_encode64() |> binary_part(0, length)
  end
end
