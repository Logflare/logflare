defmodule Logflare.Logs.Vercel.LambdaMessageParser do
  @moduledoc """
  Message examples:

  message = "START RequestId: 026080a5-4157-4f7d-8256-0b61aa0fb167 Version: $LATEST\nEND RequestId: 026080a5-4157-4f7d-8256-0b61aa0fb167\nREPORT RequestId: 026080a5-4157-4f7d-8256-0b61aa0fb167\tDuration: 17.99 ms\tBilled Duration: 100 ms\tMemory Size: 1024 MB\tMax Memory Used: 78 MB\tInit Duration: 185.18 ms\t\n"

  message1 = "START RequestId: 4d0ff57e-4022-4bfd-8689-a69e39f80f69 Version: $LATEST\n2020-02-19T17:32:52.353Z\t4d0ff57e-4022-4bfd-8689-a69e39f80f69\tINFO\tGetting metadata\n2020-02-19T17:32:52.364Z\t4d0ff57e-4022-4bfd-8689-a69e39f80f69\tINFO\tGetting projects\n2020-02-19T17:32:52.401Z\t4d0ff57e-4022-4bfd-8689-a69e39f80f69\tINFO\tGetting Logflare sources\nOh see, it handles more than one line per message\nEND RequestId: 4d0ff57e-4022-4bfd-8689-a69e39f80f69\nREPORT RequestId: 4d0ff57e-4022-4bfd-8689-a69e39f80f69\tDuration: 174.83 ms\tBilled Duration: 200 ms\tMemory Size: 1024 MB\tMax Memory Used: 84 MB\t\n"

  message3 = "START RequestId: bd8b7963-66f1-40b9-adfd-15e761cd39e8 Version: $LATEST\nEND RequestId: bd8b7963-66f1-40b9-adfd-15e761cd39e8\nREPORT RequestId: bd8b7963-66f1-40b9-adfd-15e761cd39e8\tDuration: 22.48 ms\tBilled Duration: 100 ms\tMemory Size: 1024 MB\tMax Memory Used: 85 MB\t\n"

  message4 = "START RequestId: cb510178-1382-47e8-9865-1fb954a41325 Version: $LATEST\n2020-02-22T03:40:36.354Z\tcb510178-1382-47e8-9865-1fb954a41325\tINFO\tGetting drains\n2020-02-22T03:40:36.381Z\tcb510178-1382-47e8-9865-1fb954a41325\tINFO\tLogging map\n2020-02-22T03:40:36.381Z\tcb510178-1382-47e8-9865-1fb954a41325\tINFO\tMap {\n  'a string' => \"value associated with 'a string'\",\n  {} => 'value associated with keyObj',\n  [Function: keyFunc] => 'value associated with keyFunc'\n}\n2020-02-22T03:40:36.381Z\tcb510178-1382-47e8-9865-1fb954a41325\tINFO\tGetting metadata\nEND RequestId: cb510178-1382-47e8-9865-1fb954a41325\nREPORT RequestId: cb510178-1382-47e8-9865-1fb954a41325\tDuration: 293.60 ms\tBilled Duration: 300 ms\tMemory Size: 1024 MB\tMax Memory Used: 84 MB\t\n"
  """

  def parse(message) do
    n_split_message = String.split(message, "\n")
    [_, latest] = String.split(message, "LATEST\n")

    case String.split(latest, "\nEND") do
      [lines, _] ->
        state(n_split_message, lines, message)
        |> parse_lambda_report()
        |> parse_request_id()
        |> parse_lambda_lines()
        |> Map.drop(["n_split_message", "lines_string", "og_message"])

      [_no_lines] ->
        state(n_split_message, [], message)
        |> parse_lambda_report()
        |> parse_request_id()
        |> Map.drop(["n_split_message", "lines_string", "og_message"])
    end
  end

  def parse_lambda_lines(%{"lines_string" => lines_string} = state) do
    regex =
      ~r"((?:((?:16|17|18|19|20|21)\d\d)-(0[1-9]|10|11|12)-([0-3]\d))|(?:((?:16|17|18|19|20|21)\d\d)-([0-3]\d\d)))T(([01]\d|2[0-4])(?:\:([0-5]\d)(?::([0-5]\d|60)(\.\d{1,9})?)?)?)?Z"

    lines =
      Regex.split(regex, lines_string, include_captures: true, trim: true)
      |> Enum.map(fn x -> String.split(x, "\t", trim: true) end)

    lines =
      Enum.chunk_every(lines, 2)
      |> Enum.map(fn [[t], [_id, l, m]] = _x ->
        %{"timestamp" => t, "level" => l, "message" => String.trim(m, "\n")}
      end)

    Map.put(state, "lines", lines)
  end

  def parse_request_id(%{"n_split_message" => n_split_message} = state) do
    request_id =
      n_split_message
      |> Enum.find(fn x -> String.contains?(x, "START") end)
      |> String.split(" ")
      |> Enum.at(2)

    Map.put(state, "request_id", request_id)
  end

  def parse_lambda_report(%{"n_split_message" => n_split_message} = state) do
    report =
      n_split_message
      |> Enum.find(fn x -> String.contains?(x, "REPORT") end)
      |> String.split("\t")
      |> Enum.drop_while(fn x -> String.contains?(x, "RequestId:") == true end)
      |> Enum.map(&parse_message/1)
      |> Enum.into(%{})
      |> Map.drop(["key"])

    Map.put(state, "report", report)
  end

  defp state(n_split_message, lines, message) do
    %{
      "n_split_message" => n_split_message,
      "lines_string" => lines,
      "og_message" => message,
      "request_id" => nil,
      "report" => nil,
      "lines" => nil
    }
  end

  defp parse_message(message) do
    case String.split(message, ":", trim: true) do
      [k, v] ->
        [v, kind] =
          String.trim(v)
          |> String.split(" ")

        key =
          "#{k}_#{kind}"
          |> String.downcase()
          |> String.replace(" ", "_")

        value =
          case Float.parse(v) do
            {float, _rem} -> Kernel.round(float)
            :error -> raise("Error parsing floats")
          end

        {key, value}

      _ ->
        {"key", "value"}
    end
  end
end
