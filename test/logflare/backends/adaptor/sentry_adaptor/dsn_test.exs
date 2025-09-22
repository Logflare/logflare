defmodule Logflare.Backends.Adaptor.SentryAdaptor.DSNTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.SentryAdaptor.DSN

  doctest DSN

  describe "parse/1" do
    test "successfully parses valid DSNs" do
      test_cases = [
        %{
          name: "minimal valid DSN",
          dsn: "https://public@host.com/123",
          expected_public_key: "public",
          expected_secret_key: nil,
          expected_endpoint_uri: "https://host.com/api/123/envelope/"
        },
        %{
          name: "DSN with secret key",
          dsn: "https://public:secret@host.com/123",
          expected_public_key: "public",
          expected_secret_key: "secret",
          expected_endpoint_uri: "https://host.com/api/123/envelope/"
        },
        %{
          name: "DSN with path prefix",
          dsn: "https://public@host.com/path/to/sentry/123",
          expected_public_key: "public",
          expected_secret_key: nil,
          expected_endpoint_uri: "https://host.com/path/to/sentry/api/123/envelope/"
        },
        %{
          name: "real Sentry.io DSN",
          dsn: "https://abc123@o123456.ingest.sentry.io/123456",
          expected_public_key: "abc123",
          expected_secret_key: nil,
          expected_endpoint_uri: "https://o123456.ingest.sentry.io/api/123456/envelope/"
        },
        %{
          name: "Sentry.io DSN with secret",
          dsn: "https://public:secret@o123456.ingest.sentry.io/123456",
          expected_public_key: "public",
          expected_secret_key: "secret",
          expected_endpoint_uri: "https://o123456.ingest.sentry.io/api/123456/envelope/"
        },
        %{
          name: "DSN with port in host",
          dsn: "https://public@localhost:8080/123",
          expected_public_key: "public",
          expected_secret_key: nil,
          expected_endpoint_uri: "https://localhost:8080/api/123/envelope/"
        },
        %{
          name: "DSN with empty path segments",
          dsn: "https://public@host.com//123",
          expected_public_key: "public",
          expected_secret_key: nil,
          expected_endpoint_uri: "https://host.com//api/123/envelope/"
        },
        %{
          name: "DSN with project ID with leading zeros",
          dsn: "https://public@host.com/0123",
          expected_public_key: "public",
          expected_secret_key: nil,
          expected_endpoint_uri: "https://host.com/api/0123/envelope/"
        }
      ]

      for test_case <- test_cases do
        assert {:ok, parsed} = DSN.parse(test_case.dsn),
               "Failed to parse #{test_case.name}: #{test_case.dsn}"

        assert parsed.original_dsn == test_case.dsn,
               "Original DSN mismatch for #{test_case.name}"

        assert parsed.public_key == test_case.expected_public_key,
               "Public key mismatch for #{test_case.name}"

        assert parsed.secret_key == test_case.expected_secret_key,
               "Secret key mismatch for #{test_case.name}"

        assert parsed.endpoint_uri == test_case.expected_endpoint_uri,
               "Endpoint URI mismatch for #{test_case.name}"
      end
    end

    test "handles different protocols" do
      for protocol <- ["http", "https"] do
        dsn = "#{protocol}://public@host.com/123"

        assert {:ok, parsed} = DSN.parse(dsn)
        assert parsed.endpoint_uri == "#{protocol}://host.com/api/123/envelope/"
      end
    end

    test "fails to parse invalid DSNs" do
      error_cases = [
        %{
          name: "non-string DSN (integer)",
          input: 123,
          expected_error_pattern: "expected DSN to be a string"
        },
        %{
          name: "non-string DSN (nil)",
          input: nil,
          expected_error_pattern: "expected DSN to be a string"
        },
        %{
          name: "DSN with query parameters",
          input: "https://public@host.com/123?param=value",
          expected_error_pattern: "query parameters"
        },
        %{
          name: "DSN missing user info",
          input: "https://host.com/123",
          expected_error_pattern: "missing user info"
        },
        %{
          name: "DSN missing path (project ID)",
          input: "https://public@host.com",
          expected_error_pattern: "missing project ID"
        },
        %{
          name: "DSN with non-numeric project ID",
          input: "https://public@host.com/not-a-number",
          expected_error_pattern: "expected the DSN path to end with an integer project ID"
        },
        %{
          name: "DSN with project ID having non-numeric suffix",
          input: "https://public@host.com/123abc",
          expected_error_pattern: "expected the DSN path to end with an integer project ID"
        }
      ]

      for error_case <- error_cases do
        assert {:error, error} = DSN.parse(error_case.input),
               "Expected #{error_case.name} to fail, but parsing succeeded"

        assert error =~ error_case.expected_error_pattern,
               "Error message for #{error_case.name} doesn't match expected pattern. Got: #{error}"
      end
    end
  end
end
