defmodule Logflare.Backends.Adaptor.SyslogAdaptor.SyslogTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  import Logflare.Factory

  alias Logflare.Backends.Adaptor.SyslogAdaptor.Syslog

  describe "message truncation" do
    test "truncates plaintext message to exactly max_message_bytes" do
      body_text = String.duplicate("a", 200)

      [length_str, syslog_msg] =
        build(:log_event, message: body_text)
        |> format_to_binary(%{max_message_bytes: 150})
        |> unframe()

      assert String.to_integer(length_str) == byte_size(syslog_msg)
      assert byte_size(syslog_msg) == 150
    end

    property "plaintext truncation respects max_message_bytes" do
      check all max_bytes <- integer(200..65_535),
                body_text <- string(:utf8, min_length: 10, max_length: 100_000) do
        [length_str, syslog_msg] =
          build(:log_event, message: body_text)
          |> format_to_binary(%{max_message_bytes: max_bytes})
          |> unframe()

        assert String.to_integer(length_str) == byte_size(syslog_msg)
        assert byte_size(syslog_msg) <= max_bytes
      end
    end

    property "ciphertext truncation guarantees payload fits inside max_message_bytes" do
      check all max_bytes <- integer(200..65_535),
                body_text <- string(:utf8, min_length: 10, max_length: 100_000),
                cipher_key <- binary(length: 32) do
        [length_str, syslog_msg] =
          build(:log_event, message: body_text)
          |> format_to_binary(%{max_message_bytes: max_bytes, cipher_key: cipher_key})
          |> unframe()

        assert String.to_integer(length_str) == byte_size(syslog_msg)
        assert byte_size(syslog_msg) <= max_bytes
      end
    end
  end

  defp format_to_binary(log_event, config) do
    log_event
    |> Syslog.format(config)
    |> IO.iodata_to_binary()
  end

  # splits octet-counting prefix (MSG-LEN and the space) from the actual message
  defp unframe(payload) do
    String.split(payload, " ", parts: 2)
  end
end
