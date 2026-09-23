defmodule Logflare.Endpoints.PiiRedactor do
  @moduledoc """
  Handles PII redaction for endpoint query results.

  Currently supports redaction of IP addresses in query result values.
  """

  import Logflare.Utils.Guards

  @doc """
  Redacts PII from query results based on the query's redact_pii flag.

  When redact_pii is true, this function will:
  - Replace IP addresses (IPv4 and IPv6) with "REDACTED" in all field values
  - Recursively process nested maps and lists
  - Leave field names unchanged, only redact values

  ## Examples

      iex> result = [%{"ip" => "192.168.1.1", "message" => "User 10.0.0.1 logged in"}]
      iex> Logflare.Endpoints.PiiRedactor.redact_query_result(result, true)
      [%{"ip" => "REDACTED", "message" => "User REDACTED logged in"}]

      iex> result = [%{"ip" => "192.168.1.1", "message" => "User logged in"}]
      iex> Logflare.Endpoints.PiiRedactor.redact_query_result(result, false)
      [%{"ip" => "192.168.1.1", "message" => "User logged in"}]
  """
  @spec redact_query_result(term(), boolean()) :: term()
  def redact_query_result(result, false), do: result
  def redact_query_result(result, true), do: redact_pii_from_value(result)

  @doc """
  Recursively redacts PII from any data structure.

  Handles maps, lists, and primitive values. For strings, applies IP address redaction.
  Other data types are passed through unchanged.
  """
  @spec redact_pii_from_value(term()) :: term()
  def redact_pii_from_value(%Date{} = value), do: value
  def redact_pii_from_value(%DateTime{} = value), do: value
  def redact_pii_from_value(%Time{} = value), do: value
  def redact_pii_from_value(%NaiveDateTime{} = value), do: value
  def redact_pii_from_value(%Regex{} = value), do: value
  def redact_pii_from_value(%Stream{} = value), do: value

  def redact_pii_from_value(value) when is_map(value) do
    Map.new(value, fn {key, val} -> {key, redact_pii_from_value(val)} end)
  end

  def redact_pii_from_value(value) when is_list(value) do
    Enum.map(value, &redact_pii_from_value/1)
  end

  def redact_pii_from_value(value) when is_binary(value) do
    redact_ip_addresses(value)
  end

  def redact_pii_from_value(value), do: value

  @doc """
  Redacts IP addresses from a string value.

  Replaces both IPv4 and IPv6 addresses with "REDACTED". IPv6 candidates are
  runs of address characters containing at least two colons, and only those that
  `:inet` parses as an IPv6 address are redacted, so colon-separated values such
  as times, MAC addresses and `Mod::fun` paths are left alone. A leading `key:`
  segment (one containing a non-hex character), and stray punctuation at either
  edge, stay outside the redaction. A `%zone` suffix is redacted with its address.

  ## Examples

      iex> Logflare.Endpoints.PiiRedactor.redact_ip_addresses("User 192.168.1.1 logged in")
      "User REDACTED logged in"

      iex> Logflare.Endpoints.PiiRedactor.redact_ip_addresses("IPv6: 2001:0db8:85a3::8a2e:0370:7334")
      "IPv6: REDACTED"

      iex> Logflare.Endpoints.PiiRedactor.redact_ip_addresses("Cannot parse string '2001:db8::1' as UInt8")
      "Cannot parse string 'REDACTED' as UInt8"

      iex> Logflare.Endpoints.PiiRedactor.redact_ip_addresses("started at 12:34:56 in std::vec")
      "started at 12:34:56 in std::vec"
  """
  @spec redact_ip_addresses(String.t()) :: String.t()
  def redact_ip_addresses(value) when is_binary(value) do
    value
    |> redact_ipv6_addresses()
    |> redact_ipv4_addresses()
  end

  # IPv4 regex pattern - matches xxx.xxx.xxx.xxx where xxx is 0-255
  @ipv4_regex ~r/\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b/

  @ipv6_candidate ~r/(?<![0-9A-Za-z_.%:])[0-9A-Za-z_.%]*:[0-9A-Za-z_.%]*:[0-9A-Za-z_.%:]*/
  @ipv6_leading_segment ~r/\A[0-9A-Za-z_.%]*[G-Zg-z_.%][0-9A-Za-z_.%]*:/
  @ipv6_leading_punctuation ~r/\A[._%]*/
  @ipv6_trailing_punctuation ~r/[._%]+\z/
  @ipv6_trailing_colon ~r/[._%]*:[._%]*\z/
  @ipv6_max_length 64

  @spec redact_ipv4_addresses(String.t()) :: String.t()
  defp redact_ipv4_addresses(value) do
    Regex.replace(@ipv4_regex, value, "REDACTED")
  end

  @spec redact_ipv6_addresses(String.t()) :: String.t()
  defp redact_ipv6_addresses(value) do
    Regex.replace(@ipv6_candidate, value, &redact_ipv6_token(&1, ""))
  end

  @spec redact_ipv6_token(String.t(), String.t()) :: String.t()
  defp redact_ipv6_token(token, prefix) do
    case locate_ipv6(token) do
      {lead, trail} -> prefix <> lead <> "REDACTED" <> trail
      nil -> redact_ipv6_after_leading_segment(token, prefix)
    end
  end

  @spec redact_ipv6_after_leading_segment(String.t(), String.t()) :: String.t()
  defp redact_ipv6_after_leading_segment(token, prefix) do
    case Regex.run(@ipv6_leading_segment, token) do
      [segment] ->
        rest = binary_part(token, byte_size(segment), byte_size(token) - byte_size(segment))
        redact_ipv6_token(rest, prefix <> segment)

      nil ->
        prefix <> token
    end
  end

  @spec locate_ipv6(String.t()) :: {String.t(), String.t()} | nil
  defp locate_ipv6(token) when byte_size(token) > @ipv6_max_length, do: nil

  defp locate_ipv6(token) do
    [lead] = Regex.run(@ipv6_leading_punctuation, token)
    body = binary_part(token, byte_size(lead), byte_size(token) - byte_size(lead))

    Enum.find_value(
      [nil, @ipv6_trailing_punctuation, @ipv6_trailing_colon],
      &locate_ipv6_in_body(body, lead, &1)
    )
  end

  @spec locate_ipv6_in_body(String.t(), String.t(), Regex.t() | nil) ::
          {String.t(), String.t()} | nil
  defp locate_ipv6_in_body(body, lead, trailing) do
    case split_trailing(body, trailing) do
      {candidate, trail} -> if ipv6_address?(candidate), do: {lead, trail}
      nil -> nil
    end
  end

  @spec split_trailing(String.t(), Regex.t() | nil) :: {String.t(), String.t()} | nil
  defp split_trailing(body, nil), do: {body, ""}

  defp split_trailing(body, pattern) do
    case Regex.run(pattern, body, return: :index) do
      [{start, length}] -> {binary_part(body, 0, start), binary_part(body, start, length)}
      nil -> nil
    end
  end

  @spec ipv6_address?(String.t()) :: boolean()
  defp ipv6_address?(candidate) do
    parses_as_ipv6?(candidate) or zoned_ipv6_address?(:binary.split(candidate, "%"))
  end

  @spec zoned_ipv6_address?([String.t()]) :: boolean()
  defp zoned_ipv6_address?([address, zone]) when is_non_empty_binary(zone),
    do: parses_as_ipv6?(address)

  defp zoned_ipv6_address?(_parts), do: false

  @spec parses_as_ipv6?(String.t()) :: boolean()
  defp parses_as_ipv6?(candidate) do
    match?({:ok, _address}, :inet.parse_ipv6strict_address(:binary.bin_to_list(candidate)))
  end
end
