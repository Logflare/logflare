defmodule Logflare.Endpoints.PiiRedactor do
  @moduledoc """
  Handles PII redaction for endpoint query results.

  Currently supports redaction of IP addresses in query result values.
  """

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

  Replaces both IPv4 and IPv6 addresses with "REDACTED".

  ## Examples

      iex> Logflare.Endpoints.PiiRedactor.redact_ip_addresses("User 192.168.1.1 logged in")
      "User REDACTED logged in"

      iex> Logflare.Endpoints.PiiRedactor.redact_ip_addresses("IPv6: 2001:0db8:85a3::8a2e:0370:7334")
      "IPv6: REDACTED"
  """
  @spec redact_ip_addresses(String.t()) :: String.t()
  def redact_ip_addresses(value) when is_binary(value) do
    value
    |> redact_ipv4_addresses()
    |> redact_ipv6_addresses()
  end

  # IPv4 regex pattern - matches xxx.xxx.xxx.xxx where xxx is 0-255
  @ipv4_regex ~r/\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b/

  # IPv6 regex pattern - matches various IPv6 formats
  @ipv6_regex ~r/(?:^|(?<=\s))(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))(?=\s|$)/

  @spec redact_ipv4_addresses(String.t()) :: String.t()
  defp redact_ipv4_addresses(value) do
    Regex.replace(@ipv4_regex, value, "REDACTED")
  end

  @spec redact_ipv6_addresses(String.t()) :: String.t()
  defp redact_ipv6_addresses(value) do
    Regex.replace(@ipv6_regex, value, "REDACTED")
  end
end
