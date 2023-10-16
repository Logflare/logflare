defmodule Logflare.Buffers.Buffer do
  @moduledoc """
  Defines a behaviour for a buffer.
  """

  @doc """
  Adds a list of payloads to the buffer.
  """
  @callback add_many(identifier(), payloads :: [term()]) :: :ok

  @doc """
  Clears the buffer and removes all enqueued items.
  """
  @callback clear(identifier()) :: :ok

  @doc """
  Returns the length of the buffer
  """
  @callback length(identifier()) :: non_neg_integer()

  @doc """
  Returns multiple items from the buffer
  """
  @callback pop_many(identifier(), non_neg_integer()) :: [term()]

  @doc """
  Adds payload to the buffer.
  """
  @spec add(module(), identifier(), term()) :: :ok
  def add(mod, ident, payload),
    do: mod.add_many(ident, [payload])

  @doc """
  Adds a list of payloads to the buffer.
  """
  @spec add_many(module(), identifier(), [term()]) :: :ok
  def add_many(mod, ident, payloads) when is_list(payloads),
    do: mod.add_many(ident, payloads)

  @doc """
  Clears the buffer and removes all enqueued items.
  """
  @spec clear(module(), identifier()) :: :ok
  def clear(mod, ident), do: mod.clear(ident)

  @doc """
  Returns the length of the buffer
  """
  @spec length(module(), identifier()) :: non_neg_integer()
  def length(mod, ident), do: mod.length(ident)

  @doc """
  Returns single item from the buffer
  """
  @spec pop(module(), identifier()) :: term()
  def pop(mod, ident), do: mod.pop_many(ident, 1)

  @doc """
  Returns multiple items from the buffer
  """
  @spec pop_many(module(), identifier(), non_neg_integer()) :: [term()]
  def pop_many(mod, ident, count) when is_integer(count) and count > 0,
    do: mod.pop_many(ident, count)
end
