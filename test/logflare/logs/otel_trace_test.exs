defmodule Logflare.Logs.OtelTraceTest do
  use Logflare.DataCase
  alias Logflare.Logs.OtelTrace

  describe "handle_batch/1" do
    setup do
      user = build(:user)
      source = insert(:source, user: user)
      %{resource_spans: TestUtilsGrpc.random_resource_span(), source: source}
    end

    test "attributes", %{
      resource_spans: resource_spans,
      source: source
    } do
      [%{"attributes" => a} | _] = OtelTrace.handle_batch(resource_spans, source)
      assert a != %{}
      assert Enum.any?(Map.values(a), &is_list/1)
      assert Enum.any?(Map.values(a), &is_number/1)
      assert Enum.any?(Map.values(a), &is_binary/1)
      assert Enum.any?(Map.values(a), &is_boolean/1)
    end

    test "Creates params from a list with one Resource Span that contains an Event", %{
      resource_spans: resource_spans,
      source: source
    } do
      batch = OtelTrace.handle_batch(resource_spans, source)

      span =
        resource_spans
        |> hd()
        |> then(fn rs -> rs.scope_spans end)
        |> hd()
        |> then(fn ss -> ss.spans end)
        |> hd()

      le_span = Enum.find(batch, fn params -> params["metadata"]["type"] == "span" end)

      assert le_span["trace_id"] == Base.encode16(span.trace_id, case: :lower)
      assert le_span["span_id"] == Base.encode16(span.span_id, case: :lower)
      assert le_span["parent_span_id"] == Base.encode16(span.parent_span_id, case: :lower)
      assert le_span["event_message"] == span.name

      assert le_span["timestamp"] == span.start_time_unix_nano
      assert le_span["start_time"] == span.start_time_unix_nano
      assert le_span["end_time"] == span.end_time_unix_nano

      event = hd(span.events)
      le_event = Enum.find(batch, fn params -> params["metadata"]["type"] == "event" end)

      assert le_event["event_message"] == event.name
      assert le_event["parent_span_id"] == Base.encode16(span.span_id)

      assert le_event["timestamp"] == event.time_unix_nano
    end

    test "json parsable log event body", %{
      resource_spans: resource_spans,
      source: source
    } do
      [params | _] = resource_spans |> OtelTrace.handle_batch(source)
      assert {:ok, _} = Jason.encode(params)
    end

    test "timestamps are unix nanoseconds", %{
      resource_spans: resource_spans,
      source: source
    } do
      [params | _] = OtelTrace.handle_batch(resource_spans, source)

      assert is_integer(params["timestamp"])
      assert is_integer(params["start_time"])
      assert is_integer(params["end_time"])
      assert Integer.digits(params["timestamp"]) |> length() == 19
      assert Integer.digits(params["start_time"]) |> length() == 19
      assert Integer.digits(params["end_time"]) |> length() == 19
    end

    test "correctly parses resource spans", %{
      source: source
    } do
      request = %Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceRequest{
        resource_spans: [
          %Opentelemetry.Proto.Trace.V1.ResourceSpans{
            resource: %Opentelemetry.Proto.Resource.V1.Resource{
              attributes: [
                %Opentelemetry.Proto.Common.V1.KeyValue{
                  key: "cloud.provider",
                  value: %Opentelemetry.Proto.Common.V1.AnyValue{
                    value: {:string_value, "cloudflare"}
                  }
                },
                %Opentelemetry.Proto.Common.V1.KeyValue{
                  key: "cloud.region",
                  value: %Opentelemetry.Proto.Common.V1.AnyValue{
                    value: {:string_value, "earth"}
                  }
                },
                %Opentelemetry.Proto.Common.V1.KeyValue{
                  key: "service.namespace",
                  value: %Opentelemetry.Proto.Common.V1.AnyValue{
                    value: nil
                  }
                },
                %Opentelemetry.Proto.Common.V1.KeyValue{
                  key: "service.version",
                  value: %Opentelemetry.Proto.Common.V1.AnyValue{
                    value: nil
                  }
                }
              ],
              dropped_attributes_count: 0
            },
            scope_spans: [
              %Opentelemetry.Proto.Trace.V1.ScopeSpans{
                scope: %Opentelemetry.Proto.Common.V1.InstrumentationScope{
                  name: "@microlabs/otel-cf-workers",
                  version: "",
                  attributes: [],
                  dropped_attributes_count: 0,
                  __unknown_fields__: []
                },
                spans: [
                  %Opentelemetry.Proto.Trace.V1.Span{
                    trace_id:
                      <<227, 221, 157, 247, 77, 58, 239, 142, 56, 225, 253, 181, 247, 183, 189,
                        119, 143, 30, 227, 94, 253, 211, 135, 252>>,
                    span_id: <<119, 222, 252, 119, 183, 154, 233, 230, 184, 231, 189, 188>>,
                    trace_state: "",
                    parent_span_id: <<219, 158, 187, 245, 189, 26, 225, 205, 221, 223, 141, 245>>,
                    name: "fetch POST api.logflare.app",
                    kind: :SPAN_KIND_CLIENT,
                    start_time_unix_nano: 1_751_301_254_474_000_000,
                    end_time_unix_nano: 1_751_301_254_983_000_000,
                    attributes: [
                      %Opentelemetry.Proto.Common.V1.KeyValue{
                        key: "http.request.method",
                        value: %Opentelemetry.Proto.Common.V1.AnyValue{
                          value: {:string_value, "POST"}
                        }
                      },
                      %Opentelemetry.Proto.Common.V1.KeyValue{
                        key: "network.protocol.name",
                        value: %Opentelemetry.Proto.Common.V1.AnyValue{
                          value: {:string_value, "http"}
                        }
                      },
                      %Opentelemetry.Proto.Common.V1.KeyValue{
                        key: "network.protocol.version",
                        value: %Opentelemetry.Proto.Common.V1.AnyValue{
                          value: nil
                        }
                      },
                      %Opentelemetry.Proto.Common.V1.KeyValue{
                        key: "http.request.body.size",
                        value: %Opentelemetry.Proto.Common.V1.AnyValue{
                          value: nil
                        }
                      },
                      %Opentelemetry.Proto.Common.V1.KeyValue{
                        key: "user_agent.original",
                        value: %Opentelemetry.Proto.Common.V1.AnyValue{
                          value: nil
                        }
                      },
                      %Opentelemetry.Proto.Common.V1.KeyValue{
                        key: "http.mime_type",
                        value: %Opentelemetry.Proto.Common.V1.AnyValue{
                          value: {:string_value, "application/json; charset=utf-8"}
                        }
                      },
                      %Opentelemetry.Proto.Common.V1.KeyValue{
                        key: "http.accepts",
                        value: %Opentelemetry.Proto.Common.V1.AnyValue{
                          value: nil
                        }
                      },
                      %Opentelemetry.Proto.Common.V1.KeyValue{
                        key: "http.response.status_code",
                        value: %Opentelemetry.Proto.Common.V1.AnyValue{
                          value: {:int_value, 401}
                        }
                      },
                      %Opentelemetry.Proto.Common.V1.KeyValue{
                        key: "http.response.body.size",
                        value: %Opentelemetry.Proto.Common.V1.AnyValue{
                          value: nil
                        }
                      }
                    ],
                    dropped_attributes_count: 0,
                    events: [],
                    dropped_events_count: 0,
                    links: [],
                    dropped_links_count: 0,
                    status: %Opentelemetry.Proto.Trace.V1.Status{
                      message: "",
                      code: :STATUS_CODE_UNSET,
                      __unknown_fields__: []
                    },
                    flags: 0,
                    __unknown_fields__: []
                  },
                  %Opentelemetry.Proto.Trace.V1.Span{
                    trace_id:
                      <<227, 221, 157, 247, 77, 58, 239, 142, 56, 225, 253, 181, 247, 183, 189,
                        119, 143, 30, 227, 94, 253, 211, 135, 252>>,
                    span_id: <<219, 158, 187, 245, 189, 26, 225, 205, 221, 223, 141, 245>>,
                    trace_state: "",
                    parent_span_id: "",
                    name: "fetchHandler GET",
                    kind: :SPAN_KIND_SERVER,
                    start_time_unix_nano: 1_751_301_254_474_000_000,
                    end_time_unix_nano: 1_751_301_254_983_000_000,
                    attributes: [
                      %Opentelemetry.Proto.Common.V1.KeyValue{
                        key: "faas.invocation_id",
                        value: %Opentelemetry.Proto.Common.V1.AnyValue{
                          value: nil
                        }
                      },
                      %Opentelemetry.Proto.Common.V1.KeyValue{
                        key: "http.request.body.size",
                        value: %Opentelemetry.Proto.Common.V1.AnyValue{
                          value: nil
                        }
                      },
                      %Opentelemetry.Proto.Common.V1.KeyValue{
                        key: "user_agent.original",
                        value: %Opentelemetry.Proto.Common.V1.AnyValue{
                          value:
                            {:string_value,
                             "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:139.0) Gecko/20100101 Firefox/139.0"}
                        }
                      },
                      %Opentelemetry.Proto.Common.V1.KeyValue{
                        key: "http.accepts",
                        value: %Opentelemetry.Proto.Common.V1.AnyValue{
                          value: {:string_value, "gzip, deflate, br, zstd"}
                        }
                      },
                      %Opentelemetry.Proto.Common.V1.KeyValue{
                        key: "url.full",
                        value: %Opentelemetry.Proto.Common.V1.AnyValue{
                          value: {:string_value, "http://localhost:8787/"}
                        }
                      },
                      %Opentelemetry.Proto.Common.V1.KeyValue{
                        key: "server.address",
                        value: %Opentelemetry.Proto.Common.V1.AnyValue{
                          value: {:string_value, "localhost:8787"}
                        }
                      },
                      %Opentelemetry.Proto.Common.V1.KeyValue{
                        key: "http.response.status_code",
                        value: %Opentelemetry.Proto.Common.V1.AnyValue{
                          value: {:int_value, 200}
                        }
                      },
                      %Opentelemetry.Proto.Common.V1.KeyValue{
                        key: "http.response.body.size",
                        value: %Opentelemetry.Proto.Common.V1.AnyValue{
                          value: nil
                        }
                      }
                    ],
                    dropped_attributes_count: 0,
                    events: [],
                    dropped_events_count: 0,
                    links: [],
                    dropped_links_count: 0,
                    status: %Opentelemetry.Proto.Trace.V1.Status{
                      message: "",
                      code: :STATUS_CODE_UNSET
                    },
                    flags: 0
                  }
                ],
                schema_url: ""
              }
            ],
            schema_url: ""
          }
        ]
      }

      converted =
        OtelTrace.handle_batch(request.resource_spans, source)

      assert converted
             |> Iteraptor.each(
               fn
                 {k, %_{} = v} = self ->
                   raise "contains struct"

                 {[:values | _], v} = self ->
                   raise "contains atom"

                 {[:__unknown_fields__ | _], v} = self ->
                   raise "contains atom"

                 self ->
                   self
               end,
               structs: :keep,
               yield: :all,
               keys: :reverse
             )
    end
  end
end
