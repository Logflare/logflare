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

    test "drops encrypted message when encryption overhead would exceed max_message_bytes" do
      cipher_key = :crypto.strong_rand_bytes(32)
      log_event = build(:log_event, message: String.duplicate("a", 500))

      [_length_str, headers_only] =
        log_event
        |> format_to_binary(%{max_message_bytes: 1})
        |> unframe()

      max_bytes = byte_size(headers_only) + 4

      [length_str, syslog_msg] =
        log_event
        |> format_to_binary(%{max_message_bytes: max_bytes, cipher_key: cipher_key})
        |> unframe()

      assert String.to_integer(length_str) == byte_size(syslog_msg)
      assert byte_size(syslog_msg) <= max_bytes
      assert syslog_msg == headers_only
    end

    property "plaintext truncation respects max_message_bytes" do
      check all max_bytes <- integer(1..100_000),
                body_text <- string(:utf8, min_length: 1, max_length: 100_000) do
        [length_str, syslog_msg] =
          build(:log_event, message: body_text)
          |> format_to_binary(%{max_message_bytes: max_bytes})
          |> unframe()

        assert String.to_integer(length_str) == byte_size(syslog_msg)
        assert byte_size(syslog_msg) <= max_bytes
      end
    end

    property "ciphertext truncation guarantees payload fits inside max_message_bytes" do
      cipher_key = :crypto.strong_rand_bytes(32)

      check all max_bytes <- integer(1..100_000),
                body_text <- string(:utf8, min_length: 1, max_length: 100_000) do
        [length_str, syslog_msg] =
          build(:log_event, message: body_text)
          |> format_to_binary(%{max_message_bytes: max_bytes, cipher_key: cipher_key})
          |> unframe()

        assert String.to_integer(length_str) == byte_size(syslog_msg)
        assert byte_size(syslog_msg) <= max_bytes

        # and now we ensure we can decrypt

        assert [
                 _pri_version,
                 _timestamp,
                 _hostname,
                 _app_name,
                 _procid,
                 _msgid,
                 _structured_data,
                 encrypted_message
               ] = String.split(syslog_msg, " ")

        assert <<iv::12-bytes, tag::16-bytes, ciphertext::bytes>> =
                 Base.decode64!(encrypted_message)

        assert <<_::bytes>> =
                 :crypto.crypto_one_time_aead(
                   :aes_256_gcm,
                   cipher_key,
                   iv,
                   ciphertext,
                   "syslog",
                   tag,
                   false
                 )
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
