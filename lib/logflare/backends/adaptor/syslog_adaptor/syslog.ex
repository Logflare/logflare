defmodule Logflare.Backends.Adaptor.SyslogAdaptor.Syslog do
  @moduledoc """
  Implementation of [RFC5424][] The Syslog Protocol

  It uses [octet-counting framing][].

  [RFC5424]: https://www.rfc-editor.org/rfc/rfc5424
  [octet-counting framing]: https://www.rfc-editor.org/rfc/rfc6587#section-3.4.1
  """

  alias Logflare.LogEvent

  @empty ?-

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

  @default_level Map.fetch!(@levels, "info")

  def format(log_event, cipher_key \\ nil) do
    %LogEvent{id: id, body: body} = log_event

    level = get_in(body["metadata"]["level"]) || body["level"]
    pri = 16 * 8 + severity_code(level)

    timestamp =
      body
      |> Map.fetch!("timestamp")
      |> DateTime.from_unix!(:microsecond)
      |> DateTime.truncate(:millisecond)
      |> DateTime.to_iso8601()

    metadata = body["metadata"]
    hostname = get_in(metadata["host"]) || body["host"]
    app_name = get_in(metadata["app_name"]) || body["app_name"] || "logflare"
    procid = get_in(metadata["pid"]) || body["pid"]
    msgid = id |> Ecto.UUID.dump!() |> Base.encode32(padding: false)

    msg = Jason.encode_to_iodata!(body)
    msg = if cipher_key, do: encrypt(msg, cipher_key), else: msg

    # https://datatracker.ietf.org/doc/html/rfc5424#section-6
    syslog_msg = [
      # PRI VERSION SP
      "<#{pri}>1 ",
      # TIMESTAMP
      timestamp,
      # SP
      ?\s,
      # HOSTNAME
      format_header_value(hostname, 255),
      # SP
      ?\s,
      # APP-NAME
      format_header_value(app_name, 48),
      # SP
      ?\s,
      # PROCID
      format_header_value(procid, 128),
      # SP
      ?\s,
      # MSGID
      format_header_value(msgid, 32),
      # SP
      ?\s,
      # STRUCTURED-DATA (empty for now)
      @empty,
      # SP
      ?\s
      # MSG
      | msg
    ]

    [Integer.to_string(IO.iodata_length(syslog_msg)), ?\s | syslog_msg]
  end

  defp severity_code(level) when level in 0..7, do: level
  defp severity_code(nil), do: @default_level

  defp severity_code(str) when is_binary(str) do
    str = String.downcase(str)
    Map.get(@levels, str, @default_level)
  end

  defp format_header_value(nil, _length), do: @empty

  defp format_header_value(value, length) when is_binary(value) do
    value = value |> String.replace(~r/[^\x21-\x7E]/, "_") |> String.slice(0, length)
    if value == "", do: @empty, else: value
  end

  defp format_header_value(value, length) do
    value |> to_string() |> format_header_value(length)
  end

  defp encrypt(data, key) do
    iv = :crypto.strong_rand_bytes(12)
    {ciphertext, tag} = :crypto.crypto_one_time_aead(:aes_256_gcm, key, iv, data, "syslog", true)
    Base.encode64(iv <> tag <> ciphertext)
  end
end
