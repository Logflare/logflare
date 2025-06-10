defmodule Logflare.Logs.OtelTest do
  use Logflare.DataCase
  alias Opentelemetry.Proto.Common.V1.KeyValue
  alias Opentelemetry.Proto.Common.V1.AnyValue
  alias Opentelemetry.Proto.Common.V1.ArrayValue
  alias Opentelemetry.Proto.Resource.V1.Resource
  import Logflare.Logs.Otel

  doctest Logflare.Logs.Otel
end
