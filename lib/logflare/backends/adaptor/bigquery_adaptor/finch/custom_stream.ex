defmodule Grpc.Client.Adapters.Finch.CustomStream do
  alias GRPC.Client.Adapters.Finch.StreamState

  def start() do
    with {:ok, pid} <- StreamState.start_link() do
      stream =
        Stream.unfold(pid, fn pid ->
          case StreamState.next_item(pid) do
            :close ->
              nil

            item ->
              {item, pid}
          end
        end)

      {:ok, {stream, pid}}
    end
  end

  def add_item(pid, item) do
    StreamState.add_item(pid, item)
  end

  def close(pid) do
    StreamState.close(pid)
  end
end
