defmodule Logflare.Utils.Debugging do
  @moduledoc false
  alias Logflare.Backends.IngestEventQueue

  def list_counts(source_id) do
    :erpc.multicall(
      [Node.self() | Node.list()],
      fn ->
        {Node.self(), IngestEventQueue.list_counts({source_id, nil})}
      end,
      5000
    )
  end

  def list_pending_counts(source_id) do
    :erpc.multicall(
      [Node.self() | Node.list()],
      fn ->
        {Node.self(), IngestEventQueue.list_pending_counts({source_id, nil})}
      end,
      5000
    )
  end
end
