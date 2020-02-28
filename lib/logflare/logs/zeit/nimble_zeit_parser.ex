defmodule Logflare.Logs.Zeit.NimbleLambdaMessageParser do
  @moduledoc """
  Parser for incoming Zeit Lambda messages
  """
  import NimbleParsec
  alias Logflare.JSON

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
    ignore(
      string("END RequestId: ")
      |> concat(uuid)
      |> string("\n")
    )

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
  severity = choice([string("INFO") |> replace("info"), string("WARN") |> replace("warn")])

  # Example: 2020-02-19T17:32:52.353Z\t4d0ff57e-4022-4bfd-8689-a69e39f80f69\tINFO\tGetting metadata\n

  logline =
    timestamp
    |> unwrap_and_tag("timestamp")
    |> ignore(string("\t"))
    |> ignore(uuid)
    |> ignore(string("\t"))
    |> concat(severity |> unwrap_and_tag("level"))
    |> ignore(string("\t"))
    |> concat(message |> unwrap_and_tag("message"))
    |> reduce({Map, :new, []})

  loglines =
    logline
    |> repeat()
    |> reduce({:to_loglines, []})

  json_payload =
    lookahead(string("{"))
    |> utf8_string([{:not, ?\n}], min: 2)
    |> ignore(string("\n"))
    |> reduce({:erlang, :iolist_to_binary, []})
    |> reduce({JSON, :decode!, []})

  defp to_loglines(loglines), do: loglines

  # Example: \nREPORT RequestId: 4d0ff57e-4022-4bfd-8689-a69e39f80f69\tDuration: 174.83 ms\tBilled Duration: 200 ms\tMemory Size: 1024 MB\tMax Memory Used: 84 MB\t\n
  def token_to_float([s]), do: String.to_float(s) |> round()

  number_string =
    choice([
      integer(min: 1) |> lookahead_not(string(".")),
      ascii_string([?0..?9, ?.], min: 1) |> reduce(:token_to_float)
    ])

  report =
    ignore(string("REPORT RequestId: "))
    |> ignore(uuid)
    |> ignore(string("\t"))
    |> concat(
      ignore(string("Duration: "))
      |> concat(number_string)
      |> ignore(string(" ms\t"))
      |> unwrap_and_tag("duration_ms")
    )
    |> concat(
      ignore(string("Billed Duration: "))
      |> concat(number_string)
      |> ignore(string(" ms\t"))
      |> unwrap_and_tag("billed_duration_ms")
    )
    |> concat(
      ignore(string("Memory Size: "))
      |> concat(number_string)
      |> ignore(string(" MB\t"))
      |> unwrap_and_tag("memory_size_mb")
    )
    |> concat(
      ignore(string("Max Memory Used: "))
      |> concat(number_string)
      |> ignore(string(" MB\t"))
      |> unwrap_and_tag("max_memory_used_mb")
    )
    |> ignore(optional(ascii_char([?\n])))
    |> optional(
      ignore(string("Init Duration: "))
      |> concat(number_string)
      |> ignore(optional(string("\t\n")))
      |> unwrap_and_tag("init_duration_ms")
    )
    |> reduce({Map, :new, []})

  parser =
    start
    |> unwrap_and_tag("request_id")
    |> optional(
      choice([
        json_payload |> unwrap_and_tag("data"),
        loglines |> unwrap_and_tag("lines")
      ])
    )
    |> concat(end_)
    |> concat(report |> unwrap_and_tag("report"))
    |> reduce({Map, :new, []})

  defparsecp(:do_parse, parser, inline: true)
end
