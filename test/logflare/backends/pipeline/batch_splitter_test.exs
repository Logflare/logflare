defmodule Logflare.Backends.Pipeline.BatchSplitterTest do
  use ExUnit.Case, async: true

  alias Broadway.Message
  alias Logflare.Backends.Pipeline.BatchSplitter

  defp run_splitter(messages, opts) do
    {acc, fun} = BatchSplitter.build(opts)

    Enum.map_reduce(messages, acc, fn message, acc ->
      {decision, acc} = fun.(message, acc)
      {decision, acc}
    end)
    |> elem(0)
  end

  defp message(data) do
    %Message{data: data, acknowledger: {__MODULE__, :ack_id, nil}}
  end

  test "event messages within count and byte limits" do
    messages = for n <- 1..3, do: message(%{body: %{"event_message" => "message #{n}"}})

    assert [:cont, :cont, :emit] = run_splitter(messages, max_count: 3)
  end

  test "event messages exceeding the byte limit" do
    messages = for _ <- 1..3, do: message(%{body: %{"event_message" => "a long enough message"}})

    assert [:emit | _] = run_splitter(messages, max_bytes: 10)
  end

  test "messages with {event, encoded_row} data" do
    row = ~s({"event_message":"message"}\n)
    messages = for _ <- 1..3, do: message({%{}, row})

    assert [:cont, :emit, :cont] = run_splitter(messages, max_bytes: 2 * byte_size(row))
  end
end
