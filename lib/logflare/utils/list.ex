defmodule Logflare.Utils.List do
  @doc """
  Check if `list`'s lenght is *exactly* `n`

  It does the same check as `length(list) == n`, but it will do at most `n`
  steps through the list, while `length/1` will always step through whole `list`.
  This can have negative performance impact if `lenght(list)` is much larger
  than `n`.

  ## Example

  ```elixir
  iex> #{__MODULE__}.exactly?([1, 2, 3], 2)
  false
  iex> #{__MODULE__}.exactly?([1, 2, 3], 3)
  true
  iex> #{__MODULE__}.exactly?([1, 2, 3], 4)
  false
  ```
  """
  @spec exactly?(list :: list(), n :: non_neg_integer()) :: boolean()
  def exactly?([], 0), do: true
  def exactly?([], _), do: false
  def exactly?([_ | _], 0), do: false

  def exactly?([_ | rest], n) when is_integer(n) and n > 0,
    do: exactly?(rest, n - 1)

  @doc """
  Check if `list`'s length is *at least* of `n`

  It does the same check as `length(list) >= n`, but it will do at most `n`
  steps through the list, while `length/1` will always step through whole `list`.
  This can have negative performance impact if `lenght(list)` is much larger
  than `n`.

  ## Example

  ```elixir
  iex> #{__MODULE__}.at_least?([1, 2, 3], 2)
  true
  iex> #{__MODULE__}.at_least?([1, 2, 3], 3)
  true
  iex> #{__MODULE__}.at_least?([1, 2, 3], 4)
  false
  ```
  """
  @spec at_least?(list :: list(), n :: non_neg_integer()) :: boolean()
  def at_least?([], n) when n > 0, do: false
  def at_least?(list, 0) when is_list(list), do: true

  def at_least?([_ | rest], n) when is_integer(n) and n > 0,
    do: at_least?(rest, n - 1)
end
