defmodule Logflare.Logs.Vercel.NimbleLambdaMessageParser do
  @moduledoc """
  Parser for incoming Vercel Lambda messages
  """
  import NimbleParsec
  alias Logflare.JSON

  def parse(input) do
    {:ok, [result], _, _, _, _} = do_parse(input)

    {:ok, result}
  rescue
    _e ->
      {:error,
       %{
         "parse_status" => "failed",
         "lines_string" => input
       }}
  end

  # Example: 4d0ff57e-4022-4bfd-8689-a69e39f80f69

  uuid = ascii_string([?0..?9, ?a..?f, ?-], 36) |> label("UUID")

  tab = string("\t")

  newline = string("\n")
  not_newline_char = {:not, ?\n}

  json_open = string("{")
  json_open_char = ?\{

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
    |> label("timestamp")

  # Example: START RequestId: 4d0ff57e-4022-4bfd-8689-a69e39f80f69 Version: $LATEST\n

  start =
    ignore(string("START RequestId: "))
    |> concat(uuid)
    |> ignore(string(" Version: $LATEST\n"))

  # Example: END RequestId: 4d0ff57e-4022-4bfd-8689-a69e39f80f69

  end_ =
    ignore(
      string("END RequestId: ")
      |> label("end_token")
      |> concat(uuid)
      |> optional(newline)
    )

  # Example: INFO
  severity =
    choice([
      string("INFO") |> replace("info"),
      string("WARN") |> replace("warn"),
      string("ERROR") |> replace("error")
    ])

  # Example: 2020-02-19T17:32:52.353Z\t4d0ff57e-4022-4bfd-8689-a69e39f80f69\tINFO\tGetting metadata\n

  json_line =
    lookahead_not(choice([timestamp, end_]))
    |> utf8_string([not_newline_char], min: 1)
    |> ignore(optional(newline))

  json_lines =
    json_line
    |> repeat()
    |> reduce({Enum, :join, ["\n"]})

  json_payload =
    lookahead(json_open)
    |> concat(json_lines)
    |> unwrap_and_tag(:maybe_json)

  # Example: Getting metadata\n
  message_line =
    lookahead_not(choice([timestamp, end_]))
    |> concat(
      utf8_string([not_newline_char, {:not, json_open_char}], min: 1)
      |> unwrap_and_tag(:message)
    )
    |> optional(json_payload)
    |> ignore(optional(newline))

  # Example: It also\nworks with\nseveral lines
  message =
    message_line
    |> repeat()
    |> reduce(:parse_validate_maybe_json)

  defp parse_validate_maybe_json(tokens) do
    maybe_json = Keyword.get(tokens, :maybe_json)
    message = Keyword.get_values(tokens, :message) |> Enum.join("\n")

    if maybe_json do
      maybe_json
      |> String.replace("\n", "")
      |> JSON.decode()
      |> case do
        {:ok, json} ->
          %{"data" => json, "message" => message}

        _ ->
          message = Keyword.values(tokens) |> Enum.join("")
          %{"message" => message}
      end
    else
      %{"message" => message}
    end
  end

  logline =
    timestamp
    |> unwrap_and_tag("timestamp")
    |> ignore(tab)
    |> ignore(uuid)
    |> ignore(tab)
    |> concat(severity |> unwrap_and_tag("level"))
    |> ignore(tab)
    |> concat(message |> unwrap_and_tag("message_and_data"))
    |> optional(ignore(newline))
    |> reduce({Map, :new, []})

  loglines =
    lookahead(timestamp)
    |> concat(
      logline
      |> repeat()
    )
    |> reduce({:to_loglines, []})
    |> unwrap_and_tag("lines")

  defp to_loglines(loglines) do
    for ll <- loglines do
      mad = Map.get(ll, "message_and_data")

      ll
      |> Map.drop(["message_and_data"])
      |> Map.merge(mad)
    end
  end

  any_utf8_string = utf8_string([{:not, json_open_char}], min: 1) |> unwrap_and_tag(:message)

  utf8_string_with_json =
    lookahead_not(choice([timestamp, end_]))
    |> concat(any_utf8_string)
    |> concat(json_payload)

  body =
    choice([
      loglines |> unwrap_and_tag(:loglines),
      json_payload |> reduce(:parse_json_body) |> unwrap_and_tag(:json_body),
      utf8_string_with_json
      |> reduce(:parse_validate_maybe_json)
      |> unwrap_and_tag(:maybe_string_with_json)
    ])
    |> reduce(:put_parse_status)

  def put_parse_status(loglines: lines) do
    [lines, {"parse_status", "full"}]
  end

  def put_parse_status(json_body: json_body) do
    if elem(json_body, 0) == "lines" do
      [json_body, {"parse_status", "full"}]
    else
      [json_body, {"parse_status", "partial"}]
    end
  end

  def put_parse_status(maybe_string_with_json: maybe_string_with_json) do
    if maybe_string_with_json["data"] do
      [{"lines", [maybe_string_with_json]}, {"parse_status", "full"}]
    else
      [{"lines_string", maybe_string_with_json["message"]}, {"parse_status", "partial"}]
    end
  end

  def parse_json_body([{:maybe_json, maybe_json}]) do
    maybe_json
    |> JSON.decode()
    |> case do
      {:ok, json} ->
        {"lines", [%{"data" => json}]}

      _ ->
        {"lines_string", maybe_json}
    end
  end

  defparsec :body, body

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
    |> ignore(tab)
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
    |> ignore(optional(newline |> times(max: 10)))
    |> reduce({Map, :new, []})

  parser =
    start
    |> unwrap_and_tag("request_id")
    |> optional(body)
    |> concat(end_)
    |> concat(report |> unwrap_and_tag("report"))
    |> reduce(:convert_to_map)

  def convert_to_map(tokens) do
    tokens
    |> List.flatten()
    |> Map.new()
  end

  defparsecp :do_parse, parser, inline: true
end
