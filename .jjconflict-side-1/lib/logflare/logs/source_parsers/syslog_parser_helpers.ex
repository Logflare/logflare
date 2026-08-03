defmodule Logflare.Logs.SyslogParser.Helpers do
  @moduledoc false
  alias Logflare.JSON
  import NimbleParsec
  @ascii_printable_chars [33..126]

  def byte_length do
    integer(min: 1)
    |> unwrap_and_tag(:byte_length)
    |> label("byte_length")
  end

  def version(c \\ empty()) do
    c
    |> integer(min: 1, max: 2)
    |> unwrap_and_tag(:version)
    |> label("version")
  end

  @spec priority(NimbleParsec.t()) :: NimbleParsec.t()
  def priority(c \\ empty()) do
    c
    |> ignore(string("<"))
    |> integer(min: 1, max: 3)
    |> ignore(string(">"))
    |> label("priority")
    |> reduce(:parse_priority)
  end

  def parse_priority([priority]) do
    [
      priority: priority,
      facility: decode_facility(div(priority, 8)),
      severity: decode_severity(rem(priority, 8))
    ]
  end

  @spec timestamp(NimbleParsec.t()) :: NimbleParsec.t()
  def timestamp(c \\ empty()) do
    ascii_string(c, [?0..?9, ?:, ?T, ?Z, ?., ?+, ?-], min: 20, max: 32)
    |> reduce(:parse_timestamp)
    |> unwrap_and_tag(:timestamp)
    |> label("timestamp")
  end

  def proc_id(c \\ empty()) do
    ascii_string(c, @ascii_printable_chars, min: 1, max: 128)
    |> unwrap_and_tag(:proc_id)
    |> label("proc_id")
  end

  def msg_id(c \\ empty()) do
    ascii_string(c, @ascii_printable_chars, min: 1, max: 32)
    |> unwrap_and_tag(:msg_id)
    |> label("msg_id")
  end

  def maybe(c1 \\ empty(), c) do
    choice(c1, [ignore(nilvalue()), c])
  end

  # ?= => 61
  # ?] => 93
  # ?" => 34
  # ?space => 34
  @sd_name_chars Enum.into(hd(@ascii_printable_chars), []) -- [61, 32, 93, 34]
  def sd_and_param_name(c \\ empty()) do
    ascii_string(c, @sd_name_chars,
      min: 1,
      max: 32
    )
  end

  def sd_element do
    ignore(string("["))
    |> sd_name()
    |> times(
      ignore(sp())
      |> concat(param_name())
      |> concat(ignore(string("=")))
      |> concat(param_value())
      |> wrap(),
      min: 1
    )
    |> ignore(string("]"))
    |> tag(:sd_element)
    |> label("sd_element")
    |> times(min: 1)
  end

  def param_name(c \\ empty()) do
    sd_and_param_name(c)
    |> unwrap_and_tag(:param_name)
    |> label("param_name")
  end

  def param_value(c \\ empty()) do
    c
    |> ignore(string("\""))
    |> repeat_while(
      choice([
        ~S(\") |> string() |> replace(?"),
        ~S(\\) |> string() |> replace(10),
        ~S(\]) |> string() |> replace(?]),
        utf8_char([])
      ]),
      {:not_param_value_escaped, []}
    )
    |> ignore(string("\""))
    |> reduce({List, :to_string, []})
    |> unwrap_and_tag(:param_value)
    |> label("param_value")
  end

  def sd_name(c \\ empty()) do
    sd_and_param_name(c)
    |> unwrap_and_tag(:sd_name)
    |> label("sd_name")
  end

  def message_text(c \\ empty()) do
    c
    |> utf8_string([], min: 1)
    |> reduce(:clean_maybe_parse_json)
    |> label("msg_text")
  end

  def hostname(c \\ empty()) do
    ascii_string(c, @ascii_printable_chars, min: 1, max: 255)
    |> unwrap_and_tag(:hostname)
    |> label("hostname")
  end

  def appname(c \\ empty()) do
    ascii_string(c, @ascii_printable_chars, min: 1, max: 48)
    |> unwrap_and_tag(:appname)
    |> label("appname")
  end

  def nilvalue(c \\ empty()) do
    c
    |> string("-")
    |> label("nilvalue")
  end

  def sp(c \\ empty()) do
    c
    |> string(" ")
    |> label("separator")
  end

  @spec decode_facility(integer) :: binary()
  defp decode_facility(facility) do
    case facility do
      0 -> "kernel"
      1 -> "user"
      2 -> "mail"
      3 -> "system"
      4 -> "auth"
      5 -> "syslogd"
      6 -> "line_printer"
      7 -> "network_news"
      8 -> "uucp"
      9 -> "clock"
      10 -> "auth"
      11 -> "ftp"
      12 -> "ntp"
      13 -> "log_audit"
      14 -> "log_alert"
      15 -> "clock"
      16 -> "local0"
      17 -> "local1"
      18 -> "local2"
      19 -> "local3"
      20 -> "local4"
      21 -> "local5"
      22 -> "local6"
      23 -> "local7"
    end
  end

  @spec decode_severity(integer) :: binary()
  defp decode_severity(severity) do
    case severity do
      0 -> "emergency"
      1 -> "alert"
      2 -> "critical"
      3 -> "error"
      4 -> "warning"
      5 -> "notice"
      6 -> "info"
      7 -> "debug"
    end
  end

  def parse_timestamp([timestamp]) do
    timestamp
    |> Timex.parse("{ISO:Extended}")
    |> case do
      {:ok, %DateTime{time_zone: "Etc/UTC"} = dt} ->
        dt

      {:ok, dt} ->
        Timex.to_datetime(dt, "Etc/UTC")

      {:error, _} = errtup ->
        errtup
    end
    |> DateTime.to_iso8601()
  end

  def clean_maybe_parse_json([msg_text]) do
    json_regex = ~r/([^{]*)(?<maybe_json>{.+})([^}]*)/

    with %{"maybe_json" => maybe_json} <- Regex.named_captures(json_regex, msg_text),
         {:ok, data} <- JSON.decode(maybe_json) do
      [msg_json: data, message_text: msg_text, message: clean_message_text(msg_text)]
    else
      _ -> [message_text: msg_text, message: clean_message_text(msg_text)]
    end
  end

  @spec clean_message_text(binary) :: binary
  def clean_message_text(msg_text) do
    String.replace(msg_text, ~r/\e\[[0-9;]*m(?:\e\[K)?/, "")
  end

  # ?\ => 10
  def not_param_value_escaped(<<?", _::binary>>, context, _, _), do: {:halt, context}
  def not_param_value_escaped(<<10, _::binary>>, context, _, _), do: {:halt, context}
  def not_param_value_escaped(<<?], _::binary>>, context, _, _), do: {:halt, context}
  def not_param_value_escaped(_, context, _, _), do: {:cont, context}
end
