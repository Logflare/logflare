defmodule Logflare.Backends.Adaptor.SentryAdaptor.DSNTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.SentryAdaptor.DSN

  doctest DSN

  describe "parse/1" do
    test "successfully parses minimal valid DSN" do
      dsn = "https://public@host.com/123"

      assert {:ok, parsed} = DSN.parse(dsn)
      assert parsed.original_dsn == dsn
      assert parsed.public_key == "public"
      assert parsed.secret_key == nil
      assert parsed.endpoint_uri == "https://host.com/api/123/envelope/"
    end

    test "successfully parses DSN with secret key" do
      dsn = "https://public:secret@host.com/123"

      assert {:ok, parsed} = DSN.parse(dsn)
      assert parsed.original_dsn == dsn
      assert parsed.public_key == "public"
      assert parsed.secret_key == "secret"
      assert parsed.endpoint_uri == "https://host.com/api/123/envelope/"
    end

    test "successfully parses DSN with path prefix" do
      dsn = "https://public@host.com/path/to/sentry/123"

      assert {:ok, parsed} = DSN.parse(dsn)
      assert parsed.original_dsn == dsn
      assert parsed.public_key == "public"
      assert parsed.secret_key == nil
      assert parsed.endpoint_uri == "https://host.com/path/to/sentry/api/123/envelope/"
    end

    test "successfully parses real Sentry.io DSN" do
      dsn = "https://abc123@o123456.ingest.sentry.io/123456"

      assert {:ok, parsed} = DSN.parse(dsn)
      assert parsed.original_dsn == dsn
      assert parsed.public_key == "abc123"
      assert parsed.secret_key == nil
      assert parsed.endpoint_uri == "https://o123456.ingest.sentry.io/api/123456/envelope/"
    end

    test "successfully parses Sentry.io DSN with secret" do
      dsn = "https://public:secret@o123456.ingest.sentry.io/123456"

      assert {:ok, parsed} = DSN.parse(dsn)
      assert parsed.original_dsn == dsn
      assert parsed.public_key == "public"
      assert parsed.secret_key == "secret"
      assert parsed.endpoint_uri == "https://o123456.ingest.sentry.io/api/123456/envelope/"
    end

    test "handles different protocols" do
      for protocol <- ["http", "https"] do
        dsn = "#{protocol}://public@host.com/123"

        assert {:ok, parsed} = DSN.parse(dsn)
        assert parsed.endpoint_uri == "#{protocol}://host.com/api/123/envelope/"
      end
    end

    test "handles ports in host" do
      dsn = "https://public@localhost:8080/123"

      assert {:ok, parsed} = DSN.parse(dsn)
      assert parsed.endpoint_uri == "https://localhost:8080/api/123/envelope/"
    end

    test "fails when DSN is not a string" do
      assert {:error, error} = DSN.parse(123)
      assert error =~ "expected DSN to be a string"

      assert {:error, error} = DSN.parse(nil)
      assert error =~ "expected DSN to be a string"
    end

    test "fails when DSN has query parameters" do
      dsn = "https://public@host.com/123?param=value"

      assert {:error, error} = DSN.parse(dsn)
      assert error =~ "query parameters"
    end

    test "fails when missing user info" do
      dsn = "https://host.com/123"

      assert {:error, error} = DSN.parse(dsn)
      assert error =~ "missing user info"
    end

    test "fails when missing path (project ID)" do
      dsn = "https://public@host.com"

      assert {:error, error} = DSN.parse(dsn)
      assert error =~ "missing project ID"
    end

    test "fails when project ID is not a number" do
      dsn = "https://public@host.com/not-a-number"

      assert {:error, error} = DSN.parse(dsn)
      assert error =~ "expected the DSN path to end with an integer project ID"
    end

    test "fails when project ID has non-numeric suffix" do
      dsn = "https://public@host.com/123abc"

      assert {:error, error} = DSN.parse(dsn)
      assert error =~ "expected the DSN path to end with an integer project ID"
    end

    test "handles edge case with empty path segments" do
      dsn = "https://public@host.com//123"

      assert {:ok, parsed} = DSN.parse(dsn)
      assert parsed.endpoint_uri == "https://host.com//api/123/envelope/"
    end

    test "handles project ID with leading zeros" do
      dsn = "https://public@host.com/0123"

      assert {:ok, parsed} = DSN.parse(dsn)
      assert parsed.endpoint_uri == "https://host.com/api/0123/envelope/"
    end
  end
end
