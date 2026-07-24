defmodule Logflare.Backends.Pipeline.BatchSplitter do
  @moduledoc """
  Shared Broadway `:batch_size` splitter for backend pipelines.

  Emits a batch once either a maximum message count or a maximum cumulative
  byte length is reached, whichever comes first. Byte length is measured with
  `:erlang.external_size/1` over each message's event body.

  See https://hexdocs.pm/broadway/Broadway.html#start_link/2 for the
  custom `:batch_size` contract.
  """

  alias Broadway.Message

  @max_batch_size 10_000
  @max_batch_length 8_000_000

  @doc """
  Maximum number of messages per batch, suitable for a batcher's `:max_demand`.
  """
  @spec max_batch_size() :: pos_integer()
  def max_batch_size, do: @max_batch_size

  @doc """
  Returns the `{acc, fun}` tuple for a Broadway batcher's `:batch_size` option.
  """
  @spec build() :: {tuple(), (Message.t(), tuple() -> {:emit | :cont, tuple()})}
  def build do
    {
      {@max_batch_size, @max_batch_length},
      fn
        # reached max count, emit
        _message, {1, _len} ->
          {:emit, {@max_batch_size, @max_batch_length}}

        # check content length
        message, {count, len} ->
          length = :erlang.external_size(message.data.body)

          if len - length <= 0 do
            # below max batch count, but reached max batch length
            {:emit, {@max_batch_size, @max_batch_length}}
          else
            # below max batch count, below max batch length
            {:cont, {count - 1, len - length}}
          end
      end
    }
  end
end
