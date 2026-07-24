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

  ## Examples

      iex> origin("https://cluster.example.com:9444", 8443)
      {"https", "cluster.example.com", 9444}

      iex> origin("https://cluster.example.com", 8443)
      {"https", "cluster.example.com", 8443}

      iex> origin("http://localhost", nil)
      {"http", "localhost", 8123}
  """
  @spec origin(String.t(), pos_integer() | nil) ::
          {String.t(), String.t() | nil, pos_integer() | nil}
  def origin(url, fallback_port)
      when is_non_empty_binary(url) and is_pos_integer_or_nil(fallback_port) do
    uri = URI.parse(url)
    scheme = uri.scheme || "http"
    {scheme, uri.host, resolve_port(uri, fallback_port || default_port(scheme))}
  end

  @doc """
  Resolves the effective port for an ClickHouse URI.

  ## Examples

      iex> resolve_port(URI.parse("http://host:9000"), 8123)
      9000

      iex> resolve_port(URI.parse("https://host"), 8443)
      8443

      iex> resolve_port(%URI{port: nil}, 8123)
      8123
  """
  @spec resolve_port(URI.t(), pos_integer() | nil) :: pos_integer() | nil
  def resolve_port(%URI{port: port}, fallback)
      when port in [nil, 80, 443] and is_pos_integer(fallback),
      do: fallback

  def resolve_port(%URI{port: port}, _fallback), do: port

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
end
