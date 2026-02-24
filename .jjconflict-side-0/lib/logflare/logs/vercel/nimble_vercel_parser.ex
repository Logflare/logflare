defmodule Logflare.Logs.Vercel.NimbleLambdaMessageParser do
  @moduledoc """
  Parser for incoming Vercel Lambda messages
  """

  import NimbleParsec

  alias Logflare.JSON

  require Logger

  def parse(input) do
    {:ok, [result], _, _, _, _} = do_parse(input)

    result =
      if Map.get(result, "start") do
        Map.delete(result, "start")
      else
        Map.put(result, "message_truncated", true)
      end

    {:ok, result}
  rescue
    _e ->
      :telemetry.execute(
        [:logflare, :parsers, :vercel],
        %{failed: true}
      )

      {:error,
       %{
         "parse_status" => "failed",
         "lines_string" => input
       }}
  end

  # Example: 4d0ff57e-4022-4bfd-8689-a69e39f80f69

  uuid = ascii_string([?0..?9, ?a..?f, ?-], 36) |> label("UUID")

  tab = string("\t")

  whitespace =
    choice([
      string(" "),
      string("\n"),
      string("\t")
    ])

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

  method =
    ignore(string("["))
    |> choice([
      string("GET"),
      string("POST"),
      string("PUT"),
      string("PATCH"),
      string("OPTION"),
      string("DELETE")
    ])
    |> ignore(string("] "))
    |> unwrap_and_tag("method")

  path =
    ascii_string([?0..?9, ?a..?z, ?A..?Z, ?%, ?#, ?@, ?-, ?_, ?~, ?/], min: 1)
    |> unwrap_and_tag("path")

  status =
    ignore(string(" status="))
    |> concat(integer(3))
    |> unwrap_and_tag("status")

  # Example: START RequestId: 4d0ff57e-4022-4bfd-8689-a69e39f80f69 Version: $LATEST\n

  start =
    optional(
      string("START RequestId: ")
      |> concat(uuid)
      |> optional(string(" Version: $LATEST"))
      |> optional(newline)
      |> replace(true)
      |> unwrap_and_tag("start")
    )

  # Example: END RequestId: 4d0ff57e-4022-4bfd-8689-a69e39f80f69

  end_ =
    ignore(string("END RequestId: "))
    |> label("end_token")
    |> concat(uuid)
    |> ignore(optional(newline))

  request_line =
    method
    |> concat(path)
    |> concat(status)
    |> ignore(optional(newline))

  request_lines =
    lookahead_not(choice([timestamp, end_]))
    |> concat(repeat(request_line))

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
      build_map(maybe_json, message, tokens)
    else
      %{"message" => message}
    end
  end

  defp build_map(maybe_json, message, tokens) do
    cleaned_json = String.replace(maybe_json, "\n", "")

    case JSON.decode(cleaned_json) do
      {:ok, json} ->
        %{"data" => json, "message" => message}

      {:error, _} ->
        case maybe_parse_multiline_json_body(maybe_json) do
          {"lines", lines} -> %{"multiline" => lines}
          _ -> %{"message" => tokens |> Keyword.values() |> Enum.join("")}
        end
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
      |> unwrap_and_tag(:maybe_string_with_json),
      request_lines |> tag(:request_lines)
    ])
    |> reduce(:put_parse_status)

  def put_parse_status(request_lines: lines) do
    [lines, {"parse_status", "full"}]
  end

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
    case maybe_string_with_json do
      %{"data" => _} ->
        [{"lines", [maybe_string_with_json]}, {"parse_status", "full"}]

      %{"multiline" => multiline} ->
        [{"lines", multiline}, {"parse_status", "full"}]

      _ ->
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
        maybe_parse_multiline_json_body(maybe_json)
    end
  end

  def maybe_parse_multiline_json_body(maybe_multi_json) do
    results =
      maybe_multi_json
      |> String.split("\n")
      |> Enum.map(&Jason.decode/1)
      |> Enum.filter(&match?({:ok, _}, &1))
      |> Enum.map(fn {:ok, datum} -> %{"data" => datum} end)

    if Enum.empty?(results) do
      {"lines_string", maybe_multi_json}
    else
      {"lines", results}
    end
  end

  defparsec :body, body

  # Example:
  # "\nREPORT RequestId: 4d0ff57e-4022-4bfd-8689-a69e39f80f69\t" <>
  # "Duration: 174.83 ms\tBilled Duration: 200 ms\t" <>
  # "Memory Size: 1024 MB\tMax Memory Used: 84 MB\t\n"
  def token_to_float([s]), do: String.to_float(s) |> round()

  number_string =
    choice([
      integer(min: 1) |> lookahead_not(string(".")),
      ascii_string([?0..?9, ?.], min: 1) |> reduce(:token_to_float)
    ])

  report =
    ignore(string("REPORT RequestId: "))
    |> ignore(uuid)
    |> ignore(choice([tab, string(" ")]))
    |> concat(
      ignore(string("Duration: "))
      |> concat(number_string)
      |> ignore(whitespace)
      |> ignore(string("ms"))
      |> ignore(optional(whitespace))
      |> unwrap_and_tag("duration_ms")
    )
    |> concat(
      ignore(string("Billed Duration: "))
      |> concat(number_string)
      |> ignore(string(" ms"))
      |> ignore(optional(whitespace))
      |> unwrap_and_tag("billed_duration_ms")
    )
    |> concat(
      ignore(string("Memory Size: "))
      |> concat(number_string)
      |> ignore(string(" MB"))
      |> ignore(optional(whitespace))
      |> unwrap_and_tag("memory_size_mb")
    )
    |> concat(
      ignore(string("Max Memory Used: "))
      |> concat(number_string)
      |> ignore(string(" MB"))
      |> ignore(optional(whitespace))
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
    |> optional(body)
    |> concat(
      end_
      |> unwrap_and_tag("request_id")
    )
    |> concat(report |> unwrap_and_tag("report"))
    |> reduce(:convert_to_map)

  def convert_to_map(tokens) do
    tokens
    |> List.flatten()
    |> Map.new()
  end

  defparsecp :do_parse, parser, inline: true
end
