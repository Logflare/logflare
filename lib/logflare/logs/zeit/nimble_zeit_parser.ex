defmodule Logflare.Logs.Zeit.NimbleLambdaMessageParser do
  @moduledoc """
  Parser for incoming Zeit Lambda messages
  """
  import NimbleParsec

  def parse(input) do
    {:ok, [result], _, _, _, _} = do_parse(input)

    {:ok, result}
  end

  # Example: 4d0ff57e-4022-4bfd-8689-a69e39f80f69

  uuid = ascii_string([], 36)

  # Example: 2020-02-19T17:32:52.353Z

  timestamp =
    ascii_string([], 4)
    |> string("-")
    |> ascii_string([], 2)
    |> string("-")
    |> ascii_string([], 2)
    |> string("T")
    |> ascii_string([], 2)
    |> string(":")
    |> ascii_string([], 2)
    |> string(":")
    |> ascii_string([], 2)
    |> string(".")
    |> ascii_string([], 3)
    |> string("Z")
    |> reduce({:erlang, :iolist_to_binary, []})

  # Example: START RequestId: 4d0ff57e-4022-4bfd-8689-a69e39f80f69 Version: $LATEST\n

  start =
    ignore(string("START RequestId: "))
    |> concat(uuid)
    |> ignore(string(" Version: $LATEST\n"))

  # Example: END RequestId: 4d0ff57e-4022-4bfd-8689-a69e39f80f69

  end_ =
    ignore(string("END RequestId: "))
    |> concat(ignore(uuid))
    |> ignore(string("\n"))

  # Example: Getting metadata\n

  message_line =
    lookahead_not(choice([timestamp, end_]))
    |> optional(utf8_string([{:not, ?\n}], min: 1))
    |> ignore(string("\n"))

  # Example: It also\nworks with\nseveral lines

  message =
    message_line
    |> repeat()
    |> reduce({Enum, :join, ["\n"]})

  # Example: INFO
  severity = choice([string("INFO"), string("WARN")])

  # Example: 2020-02-19T17:32:52.353Z\t4d0ff57e-4022-4bfd-8689-a69e39f80f69\tINFO\tGetting metadata\n

  logline =
    timestamp
    |> ignore(string("\t"))
    |> concat(ignore(uuid))
    |> ignore(string("\t"))
    |> concat(severity)
    |> ignore(string("\t"))
    |> concat(message)
    |> reduce({:to_logline, []})

  defp to_logline([ts, severity, message]) do
    %{
      timestamp: ts,
      level: severity,
      message: message
    }
  end

  loglines =
    logline
    |> repeat()
    |> reduce({:to_loglines, []})

  defp to_loglines(loglines), do: loglines

  # Example: \nREPORT RequestId: 4d0ff57e-4022-4bfd-8689-a69e39f80f69\tDuration: 174.83 ms\tBilled Duration: 200 ms\tMemory Size: 1024 MB\tMax Memory Used: 84 MB\t\n

  report =
    ignore(string("REPORT RequestId: "))
    |> concat(ignore(uuid))
    |> concat(
      ignore(string("\tDuration: "))
      |> ascii_string([?0..?9, ?.], min: 1)
      |> unwrap_and_tag(:duration_ms)
    )
    |> concat(
      ignore(string(" ms\tBilled Duration: "))
      |> ascii_string([?0..?9, ?.], min: 1)
      |> unwrap_and_tag(:billed_duration_ms)
    )
    |> concat(
      ignore(string(" ms\tMemory Size: "))
      |> ascii_string([?0..?9, ?.], min: 1)
      |> unwrap_and_tag(:memory_size_mb)
    )
    |> concat(
      ignore(string(" MB\tMax Memory Used: "))
      |> ascii_string([?0..?9, ?.], min: 1)
      |> unwrap_and_tag(:max_memory_used_mb)
    )
    |> ignore(choice([string(" MB\t\n"), string(" MB\t")]))
    |> optional(
      ignore(string("Init Duration: "))
      |> ascii_string([?0..?9, ?.], min: 1)
      |> unwrap_and_tag(:init_duration_ms)
    )
    |> reduce({:to_report, []})

  defp to_report(tokens) do
    Map.new(tokens)
    |> float_to_int
  end

  parser =
    start
    |> concat(loglines)
    |> concat(end_)
    |> concat(report)
    |> reduce({:to_result, []})

  defp to_result([uuid, lines, report]) do
    %{
      request_id: uuid,
      lines: lines,
      report: report
    }
  end

  defp float_to_int(map) when is_map(map) do
    Enum.map(map, fn {k, v} -> {k, float_to_int(v)} end)
    |> Enum.into(%{})
  end

  defp float_to_int(v) when is_binary(v) do
    case Float.parse(v) do
      {float, _rem} -> Kernel.round(float)
      :error -> raise("Error parsing floats")
    end
  end

  defparsecp(:do_parse, parser)
end
