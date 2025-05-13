defmodule Logflare.TestUtilsGrpc do
  alias Logflare.TestUtils

  alias Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceRequest
  alias Opentelemetry.Proto.Common.V1.AnyValue
  alias Opentelemetry.Proto.Common.V1.ArrayValue
  alias Opentelemetry.Proto.Common.V1.InstrumentationScope
  alias Opentelemetry.Proto.Common.V1.KeyValue
  alias Opentelemetry.Proto.Resource.V1.Resource
  alias Opentelemetry.Proto.Trace.V1.ScopeSpans
  alias Opentelemetry.Proto.Trace.V1.Span
  alias Opentelemetry.Proto.Trace.V1.Span.Event
  alias Opentelemetry.Proto.Trace.V1.ResourceSpans
  alias Opentelemetry.Proto.Common.V1.KeyValueList

  @doc """
  Generates a ExportTraceServiceRequest message which contains a Span and an Event in it
  """
  def random_export_service_request do
    %ExportTraceServiceRequest{
      resource_spans: random_resource_span()
    }
  end

  @doc """
  Generates a single ResourceSpan message which contains a Span and an Event in it
  """
  def random_resource_span do
    [
      %ResourceSpans{
        resource: %Resource{
          attributes: random_attributes()
        },
        scope_spans: [random_scope_span()]
      }
    ]
  end

  defp random_scope_span do
    scope = %InstrumentationScope{
      name: TestUtils.random_string(),
      version: TestUtils.random_string(),
      attributes: random_attributes()
    }

    scope_spans = %ScopeSpans{
      scope: scope,
      spans: random_span()
    }

    scope_spans
  end

  defp random_span do
    [
      %Span{
        name: TestUtils.random_string(),
        span_id: :crypto.strong_rand_bytes(8),
        parent_span_id: :crypto.strong_rand_bytes(8),
        trace_id: :crypto.strong_rand_bytes(16),
        start_time_unix_nano: DateTime.utc_now() |> DateTime.to_unix(:nanosecond),
        end_time_unix_nano: DateTime.utc_now() |> DateTime.to_unix(:nanosecond),
        events: random_event(),
        attributes: random_attributes()
      }
    ]
  end

  defp random_event do
    [
      %Event{
        name: TestUtils.random_string(),
        time_unix_nano: DateTime.utc_now() |> DateTime.to_unix(:nanosecond)
      }
    ]
  end

  defp random_attributes do
    string = random_key_value(:string)
    boolean = random_key_value(:boolean)
    integer = random_key_value(:integer)
    double = random_key_value(:double)
    array_of_strings = random_key_value(:array_of_strings)
    array_of_booleans = random_key_value(:array_of_booleans)
    array_of_integers = random_key_value(:array_of_integers)
    array_of_doubles = random_key_value(:array_of_doubles)

    [
      string,
      boolean,
      integer,
      double,
      array_of_strings,
      array_of_booleans,
      array_of_integers,
      array_of_doubles
    ]
  end

  # this is not supported by traces/metrics, only logs
  # https://github.com/open-telemetry/opentelemetry-specification/pull/2888
  # https://github.com/open-telemetry/opentelemetry-specification/issues/376
  defp random_key_value(:array_of_kv) do
    %KeyValue{
      key: "random_array_#{TestUtils.random_string()}",
      value: %AnyValue{
        value:
          {:kvlist_value,
           %KeyValueList{
             values: [
               random_key_value(:string),
               random_key_value(:boolean),
               random_key_value(:integer),
               random_key_value(:double)
             ]
           }}
      }
    }
  end

  defp random_key_value(:array_of_strings) do
    %KeyValue{
      key: "random_array_#{TestUtils.random_string()}",
      value: %AnyValue{
        value:
          {:array_value,
           %ArrayValue{
             values: [
               random_key_value(:string).value,
               random_key_value(:string).value,
               random_key_value(:string).value
             ]
           }}
      }
    }
  end

  defp random_key_value(:array_of_booleans) do
    %KeyValue{
      key: "random_array_#{TestUtils.random_string()}",
      value: %AnyValue{
        value:
          {:array_value,
           %ArrayValue{
             values: [
               random_key_value(:boolean).value,
               random_key_value(:boolean).value,
               random_key_value(:boolean).value
             ]
           }}
      }
    }
  end

  defp random_key_value(:array_of_doubles) do
    %KeyValue{
      key: "random_array_#{TestUtils.random_string()}",
      value: %AnyValue{
        value:
          {:array_value,
           %ArrayValue{
             values: [
               random_key_value(:double).value,
               random_key_value(:double).value
             ]
           }}
      }
    }
  end

  defp random_key_value(:array_of_integers) do
    %KeyValue{
      key: "random_array_#{TestUtils.random_string()}",
      value: %AnyValue{
        value:
          {:array_value,
           %ArrayValue{
             values: [
               random_key_value(:integer).value,
               random_key_value(:integer).value
             ]
           }}
      }
    }
  end

  defp random_key_value(:string) do
    %KeyValue{
      key: "random_string_#{TestUtils.random_string()}",
      value: %AnyValue{
        value: {:string_value, TestUtils.random_string()}
      }
    }
  end

  defp random_key_value(:boolean) do
    %KeyValue{
      key: "random_boolean_#{TestUtils.random_string()}",
      value: %AnyValue{value: {:bool_value, Enum.random([true, false])}}
    }
  end

  defp random_key_value(:integer) do
    %KeyValue{
      key: "random_integer_#{TestUtils.random_string()}",
      value: %AnyValue{value: {:int_value, :rand.uniform(100)}}
    }
  end

  defp random_key_value(:double) do
    %KeyValue{
      key: "random_double_#{TestUtils.random_string()}",
      value: %AnyValue{value: {:double_value, :rand.uniform(100) + 1.00}}
    }
  end
end
