defmodule Logflare.Endpoints.PiiRedactorTest do
  use ExUnit.Case, async: true

  alias Logflare.Endpoints.PiiRedactor

  describe "redact_query_result/2" do
    test "returns original result when redact_pii is false" do
      result = [%{"ip" => "192.168.1.1", "message" => "User 10.0.0.1 logged in"}]
      assert PiiRedactor.redact_query_result(result, false) == result
    end

    test "redacts IP addresses in various data structures" do
      # Simple redaction
      result = [%{"ip" => "192.168.1.1", "message" => "User 10.0.0.1 logged in"}]
      expected = [%{"ip" => "REDACTED", "message" => "User REDACTED logged in"}]
      assert PiiRedactor.redact_query_result(result, true) == expected

      # Nested maps
      nested_result = [
        %{"user" => %{"ip" => "192.168.1.1", "id" => 123}, "log" => "Connection from 10.0.0.1"}
      ]

      nested_expected = [
        %{"user" => %{"ip" => "REDACTED", "id" => 123}, "log" => "Connection from REDACTED"}
      ]

      assert PiiRedactor.redact_query_result(nested_result, true) == nested_expected

      # Lists of values
      list_result = [%{"ips" => ["192.168.1.1", "10.0.0.1"], "count" => 2}]
      list_expected = [%{"ips" => ["REDACTED", "REDACTED"], "count" => 2}]
      assert PiiRedactor.redact_query_result(list_result, true) == list_expected
    end

    test "preserves non-string data types" do
      result = [%{"timestamp" => ~D[2023-01-01], "count" => 42, "active" => true}]
      assert PiiRedactor.redact_query_result(result, true) == result
    end
  end

  describe "redact_ip_addresses/1" do
    test "redacts various IP address formats" do
      test_cases = [
        {"User connected from 192.168.1.1 and 10.0.0.1",
         "User connected from REDACTED and REDACTED"},
        {"IPv6 address: 2001:0db8:85a3:0000:0000:8a2e:0370:7334", "IPv6 address: REDACTED"},
        {"Compressed: 2001:db8::8a2e:370:7334", "Compressed: REDACTED"},
        {"IPv4: 192.168.1.1, IPv6: 2001:db8::1", "IPv4: REDACTED, IPv6: REDACTED"},
        {"IP: 192.168.1.1, not 12192.168.1.11", "IP: REDACTED, not 12192.168.1.11"},
        {"Localhost: 127.0.0.1 and ::1", "Localhost: REDACTED and REDACTED"}
      ]

      for {input, expected} <- test_cases do
        assert PiiRedactor.redact_ip_addresses(input) == expected
      end
    end

    test "handles strings without IP addresses" do
      input = "No IPs here!"
      assert PiiRedactor.redact_ip_addresses(input) == input
    end
  end

  describe "redact_pii_from_value/1" do
    test "preserves non-string primitive types" do
      assert PiiRedactor.redact_pii_from_value(nil) == nil
      assert PiiRedactor.redact_pii_from_value(42) == 42
      assert PiiRedactor.redact_pii_from_value(:test) == :test
      assert PiiRedactor.redact_pii_from_value(3.14) == 3.14
    end

    test "recursively processes nested structures" do
      input = %{"level1" => %{"level2" => ["192.168.1.1", %{"level3" => "10.0.0.1"}]}}
      expected = %{"level1" => %{"level2" => ["REDACTED", %{"level3" => "REDACTED"}]}}
      assert PiiRedactor.redact_pii_from_value(input) == expected
    end
  end
end
