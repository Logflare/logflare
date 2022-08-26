defmodule Logflare.Buffers.Buffer do
  @moduledoc """
  Defines a behaviour for a buffer.
  """
  @doc """
  Adds a payload to the buffer.
  """
  @callback add(identifier(), payload :: term()) :: :ok

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
  Returns one item from the buffer
  """
  @callback pop(identifier) :: term()

  @doc """
  Returns multiple items from the buffer
  """
  @callback pop_many(identifier(), non_neg_integer()) :: [term()]
end
