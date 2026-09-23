defmodule Logflare.Endpoints.PiiRedactorTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Logflare.Endpoints.PiiRedactor

  @separators [
    " ",
    "'",
    "\"",
    ",",
    "(",
    ")",
    "[",
    "]",
    "{",
    "}",
    "=",
    "/",
    ";",
    "\t",
    "\n",
    "<",
    ">",
    "|",
    "!",
    "?",
    "#",
    "-",
    "@",
    "*",
    "+",
    "&"
  ]
  @decoys [
    "12:34:56",
    "12:34:56.789",
    "2026-09-23T16:18:13Z",
    "aa:bb:cc:dd:ee:ff",
    "std::vec::Vec",
    "Foo::Bar.baz",
    "Elixir.Foo::bar",
    "deadbeef::cafe",
    "1::2::3",
    "key:value",
    "a:b:c",
    "ns:tag:value",
    "http:",
    "localhost:4000",
    "example.com:8443",
    "Code:",
    "DB::Exception:",
    "gg::1",
    "12345::1",
    "1:2:3:4:5:6:7:8:9",
    ":",
    "x:y:z::"
  ]

  doctest PiiRedactor

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

    test "redacts IPv6 addresses next to punctuation" do
      test_cases = [
        {"Cannot parse string '2001:db8::1' as UInt8", "Cannot parse string 'REDACTED' as UInt8"},
        {"Cannot parse string '2001:0db8:85a3:0000:0000:8a2e:0370:7334' as UInt8",
         "Cannot parse string 'REDACTED' as UInt8"},
        {~s({"ip":"2001:db8::1"}), ~s({"ip":"REDACTED"})},
        {"[2001:db8::1]:443", "[REDACTED]:443"},
        {"ip=2001:db8::1,port=443", "ip=REDACTED,port=443"},
        {"(fe80::1%eth0)", "(REDACTED)"},
        {"zone fe80::1%eth0 up", "zone REDACTED up"},
        {"ends with 2001:db8::1.", "ends with REDACTED."},
        {"from 2001:db8::1: connection refused", "from REDACTED: connection refused"},
        {"prefix 2001:db8:: only", "prefix REDACTED only"},
        {"...2001:db8::1", "...REDACTED"},
        {"path .191c:: end", "path .REDACTED end"},
        {"(.2001:db8::1:)", "(.REDACTED:)"},
        {"2001:DB8::ABCD", "REDACTED"}
      ]

      for {input, expected} <- test_cases do
        assert PiiRedactor.redact_ip_addresses(input) == expected
      end
    end

    test "keeps a leading key segment outside the redaction" do
      test_cases = [
        {"ip:2001:db8::1", "ip:REDACTED"},
        {"client.ip:2001:db8::1 ok", "client.ip:REDACTED ok"},
        {"ip:::1", "ip:REDACTED"},
        {"x:y:2001:db8::1", "x:y:REDACTED"}
      ]

      for {input, expected} <- test_cases do
        assert PiiRedactor.redact_ip_addresses(input) == expected
      end
    end

    test "redacts IPv6 addresses that embed an IPv4 address as a whole" do
      assert PiiRedactor.redact_ip_addresses("::ffff:203.0.113.5") == "REDACTED"
      assert PiiRedactor.redact_ip_addresses("'2001:db8::203.0.113.5'") == "'REDACTED'"
    end

    test "leaves colon-separated values that are not IPv6 addresses alone" do
      for decoy <- @decoys, separator <- [" ", "'", "\"", "(", "["] do
        input = "before" <> separator <> decoy <> separator <> "after"
        assert PiiRedactor.redact_ip_addresses(input) == input
      end
    end

    test "tolerates invalid UTF-8 around an address" do
      input = <<0xFF, " 2001:db8::1 ", 0xFE>>
      assert PiiRedactor.redact_ip_addresses(input) == <<0xFF, " REDACTED ", 0xFE>>
    end

    test "stays linear on long runs of address characters" do
      for input <- [
            String.duplicate("a", 1_000_000),
            String.duplicate("a:", 500_000),
            String.duplicate("ab.", 333_333) <> ":" <> String.duplicate("cd.", 333_333) <> ":",
            String.duplicate("x:y:", 250_000) <> "2001:db8::1"
          ] do
        {microseconds, _result} = :timer.tc(fn -> PiiRedactor.redact_ip_addresses(input) end)
        assert microseconds < 2_000_000
      end
    end

    property "leaves no parsable IPv6 token behind and is idempotent" do
      alphabet = String.graphemes("0123456789abcdefABCDEFxyzgip:::::..%_ '\"()[],=")

      check all chars <- list_of(member_of(alphabet), min_length: 1, max_length: 40),
                max_runs: 5_000 do
        redacted = chars |> Enum.join() |> PiiRedactor.redact_ip_addresses()

        assert PiiRedactor.redact_ip_addresses(redacted) == redacted

        for token <- Regex.scan(~r/[0-9A-Za-z_.%:]+/, redacted) |> List.flatten() do
          refute match?({:ok, _}, :inet.parse_ipv6strict_address(String.to_charlist(token)))

          refute match?(
                   {:ok, _},
                   :inet.parse_ipv6strict_address(String.to_charlist(String.trim(token, ".")))
                 )
        end
      end
    end

    property "redacts exactly the IPv6 addresses in mixed text" do
      check all parts <- list_of(part_generator(), max_length: 12), max_runs: 2_000 do
        input = Enum.map_join(parts, fn {input, _expected} -> input end)
        expected = Enum.map_join(parts, fn {_input, expected} -> expected end)

        assert PiiRedactor.redact_ip_addresses(input) == expected
      end
    end
  end

  defp part_generator do
    separator = member_of(@separators)

    token =
      one_of([
        map(ipv6_generator(), &{&1, "REDACTED"}),
        map(ipv6_generator(), &{"ip:" <> &1, "ip:REDACTED"}),
        map(member_of(@decoys), &{&1, &1})
      ])

    map({token, separator}, fn {{input, expected}, sep} -> {input <> sep, expected <> sep} end)
  end

  defp ipv6_generator do
    groups = list_of(integer(0..0xFFFF), length: 8)

    one_of([
      map(groups, fn gs -> gs |> List.to_tuple() |> :inet.ntoa() |> to_string() end),
      map(groups, fn gs -> Enum.map_join(gs, ":", &Integer.to_string(&1, 16)) end),
      map(groups, fn gs ->
        Enum.map_join(gs, ":", &String.pad_leading(Integer.to_string(&1, 16), 4, "0"))
      end),
      map({groups, member_of(["eth0", "en1", "lo"])}, fn {gs, zone} ->
        (gs |> List.to_tuple() |> :inet.ntoa() |> to_string()) <> "%" <> zone
      end),
      map({integer(0..255), integer(0..255)}, fn {a, b} -> "::ffff:10.#{a}.#{b}.1" end)
    ])
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
