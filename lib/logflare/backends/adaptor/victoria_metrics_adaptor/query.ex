defmodule Logflare.Backends.Adaptor.VictoriaMetricsAdaptor.Query do
  @moduledoc """
  Executes raw PromQL against a VictoriaMetrics read endpoint.

  Query expressions and Prometheus response data are forwarded without SQL
  parsing or result conversion. Only stored basic authentication is used.
  """

  alias Logflare.Backends.Adaptor.HttpBased.FinchPoolTimeoutNormalizer
  alias Logflare.Backends.Adaptor.HttpBased.SSRFProtection
  alias Logflare.Backends.Backend
  alias Logflare.Utils
  alias Logflare.Utils.SSRF

  @parameters ~w(time start end step timeout)
  @range_parameters ~w(start end step)
  @receive_timeout 30_000

  @type result :: {:ok, map()} | {:error, {pos_integer(), map()}}

  @spec execute(Backend.t(), String.t(), map()) :: result()
  def execute(%Backend{config: config}, query, params) do
    with :ok <- validate_query(query),
         {:ok, params} <- query_parameters(params),
         :ok <- validate_url(config[:query_url]) do
      url = String.trim_trailing(config.query_url, "/") <> query_path(params)

      client =
        Tesla.client(
          [Tesla.Middleware.Telemetry, SSRFProtection, FinchPoolTimeoutNormalizer],
          {Tesla.Adapter.Finch,
           name: Logflare.FinchDefaultHttp1, receive_timeout: @receive_timeout}
        )

      client
      |> Tesla.post(url, URI.encode_query(Map.put(params, "query", query)),
        headers: headers(config)
      )
      |> decode_response()
      |> response()
    else
      {:error, message} -> error(400, "bad_data", message)
    end
  end

  @spec validate_url(term()) :: :ok | {:error, String.t()}
  def validate_url(url) when is_binary(url) do
    case URI.new(url) do
      {:ok,
       %URI{scheme: scheme, host: host, port: port, userinfo: nil, query: nil, fragment: nil}}
      when scheme in ["http", "https"] and is_binary(host) and host != "" and
             port in 1..65_535 ->
        case SSRF.safe_resolve(host) do
          {:ok, _address} -> :ok
          {:error, reason} -> {:error, reason}
        end

      _ ->
        {:error,
         "query_url must be an HTTP(S) base URL without credentials, query parameters, or a fragment"}
    end
  end

  def validate_url(_), do: {:error, "Configure query_url on the VictoriaMetrics backend first"}

  @spec validate_query(term()) :: :ok | {:error, String.t()}
  defp validate_query(query) when is_binary(query) do
    if String.valid?(query) and String.trim(query) != "",
      do: :ok,
      else: {:error, "promql must be a non-empty UTF-8 string"}
  end

  defp validate_query(_), do: {:error, "promql must be a non-empty UTF-8 string"}

  @spec query_parameters(term()) :: {:ok, map()} | {:error, String.t()}
  defp query_parameters(params) when is_map(params) do
    params = Map.take(params, @parameters)

    with :ok <- validate_parameter_values(params),
         :ok <- validate_range(params) do
      {:ok, params}
    end
  end

  defp query_parameters(_), do: {:error, "Query parameters must be an object"}

  @spec validate_parameter_values(map()) :: :ok | {:error, String.t()}
  defp validate_parameter_values(params) do
    case Enum.find(params, fn {_key, value} -> not parameter_value?(value) end) do
      nil -> :ok
      {key, _value} -> {:error, "#{key} must be a non-empty string or a number"}
    end
  end

  @spec parameter_value?(term()) :: boolean()
  defp parameter_value?(value) when is_binary(value),
    do: String.valid?(value) and String.trim(value) != ""

  defp parameter_value?(value), do: is_number(value)

  @spec validate_range(map()) :: :ok | {:error, String.t()}
  defp validate_range(params) do
    range? = Enum.any?(@range_parameters, &Map.has_key?(params, &1))

    cond do
      range? and not Enum.all?(@range_parameters, &Map.has_key?(params, &1)) ->
        {:error, "Range queries require start, end, and step together"}

      range? and Map.has_key?(params, "time") ->
        {:error, "time cannot be combined with start, end, or step"}

      true ->
        :ok
    end
  end

  @spec query_path(map()) :: String.t()
  defp query_path(%{"start" => _start}), do: "/api/v1/query_range"
  defp query_path(_params), do: "/api/v1/query"

  @spec headers(map()) :: [{String.t(), String.t()}]
  defp headers(config) do
    headers = [
      {"accept", "application/json"},
      {"content-type", "application/x-www-form-urlencoded"}
    ]

    case Utils.encode_basic_auth(config) do
      nil -> headers
      encoded -> [{"authorization", "Basic " <> encoded} | headers]
    end
  end

  @spec decode_response(Tesla.Env.result()) :: Tesla.Env.result()
  defp decode_response({:ok, %Tesla.Env{body: body} = env}) when is_binary(body) do
    case Jason.decode(body) do
      {:ok, decoded} -> {:ok, %{env | body: decoded}}
      {:error, _reason} -> {:ok, env}
    end
  end

  defp decode_response(result), do: result

  @spec response(Tesla.Env.result()) :: result()
  defp response(
         {:ok, %Tesla.Env{status: 200, body: %{"status" => "success", "data" => data} = body}}
       )
       when is_map(data),
       do: {:ok, body}

  defp response({:ok, %Tesla.Env{status: status, body: %{"status" => "error"} = body}})
       when status in 400..599,
       do: {:error, {status, body}}

  defp response({:ok, %Tesla.Env{status: status}}) when status in [401, 403],
    do: error(status, "unauthorized", "VictoriaMetrics rejected the backend credentials")

  defp response({:ok, %Tesla.Env{status: status}}) when status in 400..599,
    do: error(status, "upstream_error", "VictoriaMetrics returned HTTP #{status}")

  defp response({:error, :pool_timeout}),
    do: error(503, "unavailable", "VictoriaMetrics query capacity is temporarily unavailable")

  defp response({:error, :timeout}),
    do: error(504, "timeout", "VictoriaMetrics query timed out")

  defp response({:error, _reason}),
    do: error(502, "unavailable", "Unable to query VictoriaMetrics")

  defp response(_response),
    do: error(502, "bad_response", "VictoriaMetrics returned an invalid query response")

  @spec error(pos_integer(), String.t(), String.t()) :: result()
  defp error(status, type, message),
    do: {:error, {status, %{"status" => "error", "errorType" => type, "error" => message}}}
end
