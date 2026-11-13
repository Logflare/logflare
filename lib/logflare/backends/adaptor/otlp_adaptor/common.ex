defmodule Logflare.Backends.Adaptor.OtlpAdaptor.Common do
  alias Logflare.Backends.Backend
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.Adaptor.OtlpAdaptor.ProtobufFormatter

  require Logger

  @logs_path "/v1/logs"

  @doc """
  HTTP client options shared by OTLP based adaptors that send flattened log records,
  gzipped, to `<endpoint>/v1/logs`.
  """
  @spec flattened_logs_client_opts(String.t(), %{String.t() => String.t()}) :: keyword()
  def flattened_logs_client_opts(endpoint, headers) do
    [
      url: Path.join(endpoint, @logs_path),
      formatter: {ProtobufFormatter, %{flatten_to_attributes: true}},
      headers: headers,
      gzip: true,
      json: false
    ]
  end

  @doc """
  A code for testing connection meant to be shared by all OTLP based adaptors.

  Sends an empty list of events, expecting success response.
  """
  @spec test_connection(module(), Backend.t()) ::
          :ok
          | {:error,
             :http_client_error | :http_server_error | :http_unknown_error | :unknown_error}
  def test_connection(client_module, %Backend{} = backend) do
    case HttpBased.Client.send_events(client_module, [], backend) do
      {:ok, %Tesla.Env{status: 200, body: %{partial_success: nil}}} ->
        :ok

      {:ok, %Tesla.Env{status: 200, body: %{partial_success: %{error_message: ""}}}} ->
        :ok

      {:ok, %Tesla.Env{status: status, body: resp_body}} when status in 400..499 ->
        Logger.warning(
          "Client error when testing OTLP backend connection: #{status} #{inspect(resp_body)}",
          backend_id: backend.id,
          user_id: backend.user_id
        )

        {:error, :http_client_error}

      {:ok, %Tesla.Env{status: status, body: resp_body}} when status in 500..599 ->
        Logger.warning(
          "Server error when testing OTLP backend connection: #{status} #{inspect(resp_body)}",
          backend_id: backend.id,
          user_id: backend.user_id
        )

        {:error, :http_server_error}

      {:ok, %Tesla.Env{status: status, body: resp_body}} ->
        Logger.warning(
          "Unknown http error #{status} when testing OTLP backend connection: #{inspect(resp_body)}",
          backend_id: backend.id,
          user_id: backend.user_id
        )

        {:error, :http_unknown_error}

      {:error, reason} ->
        Logger.warning("Request error when testing OTLP backend connection: #{inspect(reason)}",
          backend_id: backend.id,
          user_id: backend.user_id
        )

        {:error, :unknown_error}
    end
  end
end
