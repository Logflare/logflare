defmodule Logflare.Telemetry.MultiEndpointMetricsExporter do
  @moduledoc """
  `OtelMetricExporter` `:export_callback` used to fan a single metrics batch
  out to multiple OTLP collectors.

  `OtelMetricExporter` only supports one `otlp_endpoint` per instance, and
  setting `:export_callback` replaces its default HTTP export entirely. This
  module re-implements that default export (build request, gzip, POST) in a
  loop over the primary endpoint plus any configured extra endpoints, so a
  single `MetricStore`/ETS table/telemetry subscription can still ship to
  more than one destination.
  """

  alias OtelMetricExporter.OtelApi.Config
  alias OtelMetricExporter.Protocol

  @type endpoint :: %{otlp_endpoint: String.t(), otlp_headers: map()}

  @doc """
  Sends a metrics batch to the primary endpoint from `config` plus every
  endpoint in `extra_endpoints`.

  Requests run concurrently. If any endpoint fails, the whole batch is
  reported as failed so `OtelMetricExporter.MetricStore` retries it on the
  next export cycle rather than losing data for the endpoints that did fail
  — the same generation may then be resent to endpoints that already
  succeeded, which mirrors the retry/duplicate-on-retry behavior the library
  already has for a single endpoint.
  """
  @spec export_metrics({:metrics, list()}, %Config{}, [endpoint()]) :: :ok | {:error, term()}
  def export_metrics({:metrics, metrics}, %Config{} = config, extra_endpoints) do
    primary = %{otlp_endpoint: config.otlp_endpoint, otlp_headers: config.otlp_headers}

    body =
      metrics
      |> Protocol.build_metric_service_request(config.resource)
      |> Protobuf.encode_to_iodata()
      |> :zlib.gzip()

    [primary | extra_endpoints]
    |> Task.async_stream(&send_to_endpoint(&1, body), timeout: :timer.seconds(30))
    |> Enum.map(fn
      {:ok, result} -> result
      {:exit, reason} -> {:error, reason}
    end)
    |> collect_result()
  end

  @spec send_to_endpoint(endpoint(), iodata()) :: :ok | {:error, {String.t(), term()}}
  defp send_to_endpoint(%{otlp_endpoint: endpoint, otlp_headers: extra_headers}, body) do
    request = Finch.build(:post, endpoint <> "/v1/metrics", request_headers(extra_headers), body)

    case Finch.request(request, OtelMetricExporter.Finch) do
      {:ok, %{status: status}} when status in 200..202 -> :ok
      {:ok, response} -> {:error, {endpoint, {:unexpected_status, response}}}
      {:error, reason} -> {:error, {endpoint, reason}}
    end
  end

  @spec request_headers(map()) :: [{String.t(), String.t()}]
  defp request_headers(extra_headers) do
    %{
      "content-type" => "application/x-protobuf",
      "accept" => "application/x-protobuf",
      "content-encoding" => "gzip"
    }
    |> Map.merge(extra_headers)
    |> Map.to_list()
  end

  @spec collect_result([:ok | {:error, term()}]) :: :ok | {:error, term()}
  defp collect_result(results) do
    case Enum.filter(results, &match?({:error, _}, &1)) do
      [] -> :ok
      errors -> {:error, errors}
    end
  end
end
