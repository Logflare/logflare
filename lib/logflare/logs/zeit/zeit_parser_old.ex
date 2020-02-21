defmodule Logflare.Logs.Zeit.OldLambdaMessageParser do
  def parse_lambda_message(message) do
    n_split_message = String.split(message, "\n")
    [_, latest] = String.split(message, "LATEST\n")
    [lines, _] = String.split(latest, "\nEND")

    %{
      n_split_message: n_split_message,
      lines_string: lines,
      og_message: message,
      request_id: "",
      report: %{},
      lines: []
    }
    |> parse_lambda_report()
    |> parse_request_id()
    |> parse_lambda_lines()
    |> Map.drop([:n_split_message, :lines_string, :og_message])
  end

  def parse_lambda_lines(%{lines_string: lines_string} = state) do
    regex =
      ~r"((?:((?:16|17|18|19|20|21)\d\d)-(0[1-9]|10|11|12)-([0-3]\d))|(?:((?:16|17|18|19|20|21)\d\d)-([0-3]\d\d)))T(([01]\d|2[0-4])(?:\:([0-5]\d)(?::([0-5]\d|60)(\.\d{1,9})?)?)?)?Z"

    lines =
      Regex.split(regex, lines_string, include_captures: true, trim: true)
      |> Enum.map(fn x -> String.split(x, "\t", trim: true) end)

    lines =
      for [t] <- lines,
          [_request_id, l, m] <- lines,
          do: %{timestamp: t, level: l, message: m}

    %{state | lines: lines}
  end

  def parse_request_id(%{n_split_message: n_split_message} = state) do
    request_id =
      n_split_message
      |> Enum.find(fn x -> String.contains?(x, "START") end)
      |> String.split(" ")
      |> Enum.at(2)

    %{state | request_id: request_id}
  end

  def parse_lambda_report(%{n_split_message: n_split_message} = state) do
    report =
      n_split_message
      |> Enum.find(fn x -> String.contains?(x, "REPORT") end)
      |> String.split("\t")
      |> Enum.drop_while(fn x -> String.contains?(x, "RequestId:") == true end)
      |> Enum.map(fn x ->
        case String.split(x, ":", trim: true) do
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
      end)
      |> Enum.into(%{})
      |> Map.drop(["key"])

    %{state | report: report}
  end
end
