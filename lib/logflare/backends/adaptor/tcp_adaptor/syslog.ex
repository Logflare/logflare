defmodule Logflare.Backends.Adaptor.TCPAdaptor.Syslog do
  @moduledoc """
  Implementation of [RFC5424 The Syslog Protocol][]

  It uses [octet-counting framing][].

  [RFC5424]: https://www.rfc-editor.org/rfc/rfc5424
  [octet-counting framing]: https://www.rfc-editor.org/rfc/rfc6587#section-3.4.1
  """

  alias Logflare.LogEvent

  def format(log_event) do
    %LogEvent{body: body} = log_event

    level = get_in(body["body"]["level"]) || body["level"] || "info"
    severity_code = severity_code(level)

    timestamp =
      body
      |> Map.fetch!("timestamp")
      |> DateTime.from_unix!(:microsecond)
      |> DateTime.truncate(:millisecond)
      |> DateTime.to_iso8601()

    # TODO
    hostname = "hostname"
    app_name = "app_name"
    procid = "procid"
    id = "msgid"

    msg = [
      # PRI VERSION SP
      "<#{16 * 8 + severity_code}>1 ",
      # TIMESTAMP
      timestamp,
      # SP
      ?\s,
      # HOSTNAME
      hostname,
      # SP
      ?\s,
      # APP-NAME
      app_name,
      # SP
      ?\s,
      # PROCID
      procid,
      # SP
      ?\s,
      # MSGID
      id,
      # SP
      ?\s,
      # STRUCTURED-DATA
      ?-,
      # SP
      ?\s
      # MSG
      | Jason.encode_to_iodata!(body)
    ]

    [Integer.to_string(IO.iodata_length(msg)), ?\s | msg]
  end

  @levels %{
    "emergency" => 0,
    "emer" => 0,
    "alert" => 1,
    "critical" => 2,
    "crit" => 2,
    "error" => 3,
    "err" => 3,
    "warning" => 4,
    "warn" => 4,
    "notice" => 5,
    "informational" => 6,
    "info" => 6,
    "debug" => 7
  }

  defp severity_code(level) when level in 0..7, do: level

  defp severity_code(str) when is_binary(str) do
    str = String.downcase(str)
    Map.fetch!(@levels, str)
  end
end
