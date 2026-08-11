defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.EndpointUtils do
  @moduledoc """
  Helpers for interpreting ClickHouse endpoint URLs.
  """

  import Logflare.Utils.Guards

  @clickhouse_cloud_suffix ".clickhouse.cloud"

  defguardp is_pos_integer_or_nil(value) when is_pos_integer(value) or is_nil(value)

  @doc """
  Returns the ClickHouse default port for a URL scheme.

  ## Examples

      iex> default_port("https")
      8443

      iex> default_port("http")
      8123
  """
  @spec default_port(String.t()) :: pos_integer()
  def default_port("https"), do: 8443
  def default_port(_scheme), do: 8123

  @doc """
  Resolves the request origin `{scheme, host, port}` for a ClickHouse URL.

  An explicitly-specified port is always honored — including the standard `80`/`443`. When
  the URL omits a port, `fallback_port` is used, falling back to the ClickHouse scheme default.

  ## Examples

      iex> origin("https://cluster.example.com:9444", 8443)
      {"https", "cluster.example.com", 9444}

      iex> origin("https://cluster.example.com", 8443)
      {"https", "cluster.example.com", 8443}

      iex> origin("http://localhost:80", 8123)
      {"http", "localhost", 80}

      iex> origin("http://localhost", nil)
      {"http", "localhost", 8123}
  """
  @spec origin(String.t(), pos_integer() | nil) ::
          {String.t(), String.t() | nil, pos_integer() | nil}
  def origin(url, fallback_port)
      when is_non_empty_binary(url) and is_pos_integer_or_nil(fallback_port) do
    uri = URI.parse(url)
    scheme = uri.scheme || "http"
    {scheme, uri.host, explicit_port(url) || fallback_port || default_port(scheme)}
  end

  @doc """
  Extracts the host from a URL, or `nil` when the URL is blank or has no parsable host.

  ## Examples

      iex> host("https://cluster.example.com:8443")
      "cluster.example.com"

      iex> host("not-a-url")
      nil

      iex> host(nil)
      nil
  """
  @spec host(term()) :: String.t() | nil
  def host(url) when is_non_empty_binary(url) do
    case URI.new(url) do
      {:ok, %URI{host: host}} when is_non_empty_binary(host) -> host
      _ -> nil
    end
  end

  def host(_url), do: nil

  @doc """
  Removes any basic auth userinfo from a URL.

  ClickHouse basic auth is not supported, so credentials are dropped when config is saved
  rather than being persisted.

  ## Examples

      iex> strip_credentials("http://user:pa55@cluster.example.com:8123")
      "http://cluster.example.com:8123"

      iex> strip_credentials("http://user@cluster.example.com:8123")
      "http://cluster.example.com:8123"

      iex> strip_credentials("https://cluster.example.com:8443")
      "https://cluster.example.com:8443"

      iex> strip_credentials(nil)
      nil
  """
  @spec strip_credentials(url) :: url when url: term()
  def strip_credentials(url) when is_non_empty_binary(url) do
    case URI.parse(url) do
      %URI{userinfo: nil} -> url
      %URI{} = uri -> URI.to_string(%{uri | userinfo: nil})
    end
  end

  def strip_credentials(url), do: url

  @doc """
  Returns true when the URL host appears to be a ClickHouse Cloud endpoint.

  ## Examples

      iex> clickhouse_cloud_url?("https://abc123.us-east-1.aws.clickhouse.cloud:8443")
      true

      iex> clickhouse_cloud_url?("https://xyz.europe-west4.gcp.clickhouse.cloud")
      true

      iex> clickhouse_cloud_url?("http://localhost:8123")
      false

      iex> clickhouse_cloud_url?("https://clickhouse.cloud.evil.com")
      false

      iex> clickhouse_cloud_url?(nil)
      false
  """
  @spec clickhouse_cloud_url?(term()) :: boolean()
  def clickhouse_cloud_url?(url) when is_non_empty_binary(url) do
    case host(url) do
      host when is_non_empty_binary(host) -> String.ends_with?(host, @clickhouse_cloud_suffix)
      _ -> false
    end
  end

  def clickhouse_cloud_url?(_url), do: false

  # `URI.parse/1` normalizes both an omitted port and an explicitly-supplied standard port
  # (80/443) to the same value, losing that distinction. `:uri_string.parse/1` reports
  # `:port` only when the URL actually carries one, so an explicit 80/443 is preserved
  # rather than being mistaken for "absent" and overwritten with the fallback.
  @spec explicit_port(String.t()) :: :inet.port_number() | nil
  defp explicit_port(url) do
    case :uri_string.parse(url) do
      %{port: port} -> port
      _ -> nil
    end
  end
end
