defmodule Logflare.Logs.Zeit.NimbleLambdaMessageParser do
  import NimbleParsec

  def parse(input) do
    {:ok, [result], _, _, _, _} = do_parse(input)

    {:ok, result}
  end

  def test() do
    {:ok,
     %{
       "lines" => [
         %{
           "message" => "Getting metadata",
           "level" => "INFO",
           "timestamp" => "2020-02-19T17:32:52.353Z"
         },
         %{
           "message" => "Getting projects",
           "level" => "INFO",
           "timestamp" => "2020-02-19T17:32:52.364Z"
         },
         %{
           "message" =>
             "Getting Logflare sources\nOh see, it handles more than one line per message",
           "level" => "INFO",
           "timestamp" => "2020-02-19T17:32:52.401Z"
         }
       ],
       "report" => %{
         "billed_duration_ms" => 175,
         "duration_ms" => 200,
         "max_memory_used_mb" => 1024,
         "memory_size_mb" => 84
       },
       "request_id" => "4d0ff57e-4022-4bfd-8689-a69e39f80f69"
     }} == parse(test_input())
  end

  def test_input() do
    "START RequestId: 4d0ff57e-4022-4bfd-8689-a69e39f80f69 Version: $LATEST\n2020-02-19T17:32:52.353Z\t4d0ff57e-4022-4bfd-8689-a69e39f80f69\tINFO\tGetting metadata\n2020-02-19T17:32:52.364Z\t4d0ff57e-4022-4bfd-8689-a69e39f80f69\tINFO\tGetting projects\n2020-02-19T17:32:52.401Z\t4d0ff57e-4022-4bfd-8689-a69e39f80f69\tINFO\tGetting Logflare sources\nOh see, it handles more than one line per message\nEND RequestId: 4d0ff57e-4022-4bfd-8689-a69e39f80f69\nREPORT RequestId: 4d0ff57e-4022-4bfd-8689-a69e39f80f69\tDuration: 174.83 ms\tBilled Duration: 200 ms\tMemory Size: 1024 MB\tMax Memory Used: 84 MB\t\n"

    "START RequestId: 026080a5-4157-4f7d-8256-0b61aa0fb167 Version: $LATEST\nEND RequestId: 026080a5-4157-4f7d-8256-0b61aa0fb167\nREPORT RequestId: 026080a5-4157-4f7d-8256-0b61aa0fb167\tDuration: 17.99 ms\tBilled Duration: 100 ms\tMemory Size: 1024 MB\tMax Memory Used: 78 MB\tInit Duration: 185.18 ms\t\n"
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
      "timestamp" => ts,
      "level" => severity,
      "message" => message
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
      "billed_duration_ms" => duration,
      "duration_ms" => billed_duration,
      "max_memory_used_mb" => memory_size,
      "memory_size_mb" => max_memory_used
    }
    |> float_to_int()
  end

  parser =
    start
    |> concat(loglines)
    |> concat(end_)
    |> concat(report)
    |> reduce({:to_result, []})

  defp to_result([uuid, lines, report]) do
    %{
      "request_id" => uuid,
      "lines" => lines,
      "report" => report
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
