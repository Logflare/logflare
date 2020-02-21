defmodule Logflare.Logs.Zeit.LambdaMessageParser do
  import NimbleParsec

  def parse(input) do
    {:ok, [result], _, _, _, _} = do_parse(input)
    {:ok, result}
  end

  def test() do
    {:ok,
     %{
       lines: [
         %{
           message: "Getting metadata",
           severity: "INFO",
           timestamp: "2020-02-19T17:32:52.353Z"
         },
         %{
           message: "Getting projects",
           severity: "INFO",
           timestamp: "2020-02-19T17:32:52.364Z"
         },
         %{
           message: "Getting Logflare sources\nOh see, it handles more than one line per message",
           severity: "INFO",
           timestamp: "2020-02-19T17:32:52.401Z"
         }
       ],
       report: %{
         "billed_duration_ms" => "174.83",
         "duration_ms" => "200",
         "max_memory_used_mb" => "1024",
         "memory_size_mb" => "84"
       },
       request_id: "4d0ff57e-4022-4bfd-8689-a69e39f80f69"
     }} == parse(test_input())
  end

  def test_input() do
    "START RequestId: 4d0ff57e-4022-4bfd-8689-a69e39f80f69 Version: $LATEST\n2020-02-19T17:32:52.353Z\t4d0ff57e-4022-4bfd-8689-a69e39f80f69\tINFO\tGetting metadata\n2020-02-19T17:32:52.364Z\t4d0ff57e-4022-4bfd-8689-a69e39f80f69\tINFO\tGetting projects\n2020-02-19T17:32:52.401Z\t4d0ff57e-4022-4bfd-8689-a69e39f80f69\tINFO\tGetting Logflare sources\nOh see, it handles more than one line per message\nEND RequestId: 4d0ff57e-4022-4bfd-8689-a69e39f80f69\nREPORT RequestId: 4d0ff57e-4022-4bfd-8689-a69e39f80f69\tDuration: 174.83 ms\tBilled Duration: 200 ms\tMemory Size: 1024 MB\tMax Memory Used: 84 MB\t\n"
  end

  # Example: 4d0ff57e-4022-4bfd-8689-a69e39f80f69
  uuid = ascii_string([?0..?9, ?a..?z, ?-], min: 1)

  # Example: 2020-02-19T17:32:52.353Z
  timestamp = ascii_string([?0..?9, ?-, ?:, ?., ?T, ?Z], 24)

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

  # Example: 2020-02-19T17:32:52.353Z\t4d0ff57e-4022-4bfd-8689-a69e39f80f69\tINFO\tGetting metadata\n
  logline =
    timestamp
    |> ignore(string("\t"))
    |> concat(ignore(uuid))
    |> ignore(string("\t"))
    |> ascii_string([?A..?Z], min: 1)
    |> ignore(string("\t"))
    |> concat(message)
    |> reduce({:to_logline, []})

  defp to_logline([ts, severity, message]) do
    %{
      timestamp: ts,
      severity: severity,
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
    |> ignore(string("\tDuration: "))
    |> ascii_string([?0..?9, ?.], min: 1)
    |> ignore(string(" ms\tBilled Duration: "))
    |> ascii_string([?0..?9, ?.], min: 1)
    |> ignore(string(" ms\tMemory Size: "))
    |> ascii_string([?0..?9, ?.], min: 1)
    |> ignore(string(" MB\tMax Memory Used: "))
    |> ascii_string([?0..?9, ?.], min: 1)
    |> ignore(string(" MB\t\n"))
    |> reduce({:to_report, []})

  defp to_report([duration, billed_duration, memory_size, max_memory_used]) do
    %{
      "billed_duration_ms" => float_to_int(duration),
      "duration_ms" => float_to_int(billed_duration),
      "max_memory_used_mb" => float_to_int(memory_size),
      "memory_size_mb" => float_to_int(max_memory_used)
    }
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

  defp float_to_int(v) do
    case Float.parse(v) do
      {float, _rem} -> Kernel.round(float)
      :error -> raise("Error parsing floats")
    end
  end

  defparsecp(:do_parse, parser)
end
