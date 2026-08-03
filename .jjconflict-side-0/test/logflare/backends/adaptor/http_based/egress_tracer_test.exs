defmodule Logflare.Backends.Adaptor.HttpBased.EgressTracerTest do
  use Logflare.DataCase, async: false

  require Record

  alias Logflare.Backends.Adaptor.HttpBased.EgressTracer

  @span_fields Record.extract(:span, from: "deps/opentelemetry/include/otel_span.hrl")
  Record.defrecordp(:span, @span_fields)

  setup do
    :ok = :otel_simple_processor.set_exporter(:otel_exporter_pid, self())
    on_exit(fn -> :otel_simple_processor.set_exporter(:none) end)
    :ok
  end

  defp run(response, opts \\ []) do
    env = %Tesla.Env{
      method: :post,
      url: "https://example.com",
      headers: [{"content-type", "application/json"}],
      body: "payload",
      opts: opts
    }

    next = [{:fn, fn _env -> response end}]
    EgressTracer.call(env, next, [])
  end

  defp collect_span do
    receive do
      {:span, s} -> s
    after
      0 -> flunk("expected an :http_egress span to be exported")
    end
  end

  defp attrs(s), do: :otel_attributes.map(span(s, :attributes))

  test "records request attributes on the span" do
    run({:ok, %Tesla.Env{status: 202, body: "ok"}})

    s = collect_span()
    assert span(s, :name) == :http_egress

    attributes = attrs(s)
    assert attributes[:body_length] == byte_size("payload")
    assert attributes[:request_length] == attributes[:body_length] + attributes[:headers_length]
  end

  test "records response status code and length for a successful response" do
    run({:ok, %Tesla.Env{status: 202, body: "accepted"}})

    s = collect_span()
    attributes = attrs(s)
    assert attributes[:response_status_code] == 202
    assert attributes[:response_length] == byte_size("accepted")
    assert span(s, :status) == :undefined
  end

  test "marks the span as errored on a non-2xx response" do
    run({:ok, %Tesla.Env{status: 500, body: "boom"}})

    s = collect_span()
    attributes = attrs(s)
    assert attributes[:response_status_code] == 500
    assert {:status, :error, "HTTP 500"} = span(s, :status)
  end

  test "marks the span as errored on a transport error" do
    run({:error, :econnrefused})

    s = collect_span()
    attributes = attrs(s)
    assert attributes[:error] == ":econnrefused"
    assert {:status, :error, ":econnrefused"} = span(s, :status)
  end

  test "passes the result through unchanged" do
    assert {:ok, %Tesla.Env{status: 202}} = run({:ok, %Tesla.Env{status: 202, body: "ok"}})
    assert {:error, :econnrefused} = run({:error, :econnrefused})
  end
end
