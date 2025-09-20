defmodule Logflare.Backends.Adaptor.SentryAdaptor.DSN do
  @moduledoc """
  Parser for Sentry DSN strings to extract endpoint and authentication information.
  See: https://develop.sentry.dev/sdk/overview/#parsing-the-dsn
  """

  @type t() :: %__MODULE__{
          original_dsn: String.t(),
          endpoint_uri: String.t(),
          public_key: String.t(),
          secret_key: String.t() | nil
        }

  defstruct [
    :original_dsn,
    :endpoint_uri,
    :public_key,
    :secret_key
  ]

  @doc """
  Parses a Sentry DSN string and returns the parsed DSN struct.

  ## Examples

      iex> DSN.parse("https://key@sentry.io/123")
      {:ok, %DSN{endpoint_uri: "https://sentry.io/api/123/envelope/", public_key: "key", secret_key: nil, original_dsn: "https://key@sentry.io/123"}}
  """
  @spec parse(String.t()) :: {:ok, t()} | {:error, String.t()}
  def parse(dsn) when is_binary(dsn) do
    uri = URI.parse(dsn)

    if uri.query do
      throw("DSN with query parameters is not supported. Please remove query parameters from the DSN.")
    end

    unless is_binary(uri.path) do
      throw("missing project ID at the end of the DSN URI: #{inspect(dsn)}")
    end

    unless is_binary(uri.userinfo) do
      throw("missing user info in the DSN URI: #{inspect(dsn)}")
    end

    {public_key, secret_key} =
      case String.split(uri.userinfo, ":", parts: 2) do
        [public, secret] -> {public, secret}
        [public] -> {public, nil}
      end

    with {:ok, {base_path, project_id}} <- pop_project_id(uri.path) do
      new_path = Enum.join([base_path, "api", project_id, "envelope"], "/") <> "/"
      endpoint_uri = %URI{uri | userinfo: nil, path: new_path}

      parsed_dsn = %__MODULE__{
        endpoint_uri: URI.to_string(endpoint_uri),
        public_key: public_key,
        secret_key: secret_key,
        original_dsn: dsn
      }

      {:ok, parsed_dsn}
    end
  catch
    message -> {:error, message}
  end

  def parse(other) do
    {:error, "expected DSN to be a string, got: #{inspect(other)}"}
  end

  ## Helpers

  defp pop_project_id(uri_path) do
    path = String.split(uri_path, "/")
    {project_id, path} = List.pop_at(path, -1)

    case Integer.parse(project_id) do
      {_project_id, ""} ->
        {:ok, {Enum.join(path, "/"), project_id}}

      _other ->
        {:error, "expected the DSN path to end with an integer project ID, got: #{inspect(uri_path)}"}
    end
  end
end
