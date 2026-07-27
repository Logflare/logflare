defmodule Logflare.LogEventTest do
  @moduledoc false
  use Logflare.DataCase
  use ExUnitProperties

  alias Logflare.LogEvent
  alias Logflare.Sources.Source
  alias Logflare.Utils

  @subject LogEvent

  setup do
    user = insert(:user)
    source = insert(:source, user_id: user.id)
    [source: source, user: user]
  end

  @vallog_event_ids %{"event_message" => "something", "metadata" => %{"my" => "key"}}
  test "make/2 from valid params", %{source: source} do
    assert %LogEvent{
             body: body,
             drop: false,
             id: id,
             ingested_at: _,
             is_from_stale_query: nil,
             source_id: source_id,
             valid: true,
             pipeline_error: nil,
             via_rule_id: nil
           } = LogEvent.make(@vallog_event_ids, %{source: source})

    assert id == body["id"]
    assert body["metadata"]["my"] == "key"
    assert source_id == source.id
  end

  describe "bigquery_spec" do
    test "dashes to underscores", %{source: source} do
      assert %LogEvent{
               body: %{
                 "_test_field" => _
               }
             } = LogEvent.make(%{"test-field" => 123}, %{source: source})
    end
  end

  describe "copy_fields" do
    test "nested", %{source: source} do
      source =
        %{
          source
          | transform_copy_fields: """
              food:my.field
            """
        }
        |> Source.parse_copy_fields_config()

      assert %LogEvent{
               body: %{
                 "my" => %{
                   "field" => 123
                 },
                 "food" => _
               }
             } = LogEvent.make(%{"food" => 123}, %{source: source})
    end

    test "works with unparsed fallback", %{source: source} do
      source = %{
        source
        | transform_copy_fields: """
            food:my.field
          """
      }

      assert %LogEvent{
               body: %{
                 "my" => %{
                   "field" => 123
                 },
                 "food" => _
               }
             } = LogEvent.make(%{"food" => 123}, %{source: source})
    end

    test "top level", %{source: source} do
      source =
        %{
          source
          | transform_copy_fields: """
              food:field
            """
        }
        |> Source.parse_copy_fields_config()

      assert %LogEvent{
               body: %{
                 "field" => 123,
                 "food" => 123
               }
             } = LogEvent.make(%{"food" => 123}, %{source: source})
    end

    test "multiple", %{source: source} do
      source =
        %{
          source
          | transform_copy_fields: """
              food:field
              field:123
            """
        }
        |> Source.parse_copy_fields_config()

      assert %LogEvent{
               body: %{
                 "123" => 123,
                 "field" => 123,
                 "food" => 123
               }
             } = LogEvent.make(%{"food" => 123}, %{source: source})
    end

    test "dashes in field", %{source: source} do
      source =
        %{
          source
          | transform_copy_fields: """
              _my_food:field
            """
        }
        |> Source.parse_copy_fields_config()

      assert %LogEvent{body: body} = LogEvent.make(%{"my-food" => 123}, %{source: source})
      assert Map.drop(body, ["id", "timestamp"]) == %{"_my_food" => 123, "field" => 123}
    end

    test "whitespace-only config is a no-op", %{source: source} do
      source =
        %{source | transform_copy_fields: "   \n\t\n   "}
        |> Source.parse_copy_fields_config()

      assert source.transform_copy_fields_parsed == []

      assert %LogEvent{body: body} = LogEvent.make(%{"food" => 123}, %{source: source})
      assert Map.drop(body, ["id", "timestamp"]) == %{"food" => 123}
    end

    test "invalid instructions are ignored", %{source: source} do
      source =
        %{
          source
          | transform_copy_fields: """
              food field
              food-field
              foodfield
              missing:field
            """
        }
        |> Source.parse_copy_fields_config()

      assert %LogEvent{body: body} = LogEvent.make(%{"food" => 123}, %{source: source})
      assert Map.drop(body, ["id", "timestamp"]) == %{"food" => 123}
    end

    test "preserves existing sibling keys at nested destination", %{source: source} do
      source =
        %{source | transform_copy_fields: "food:metadata.flat.field"}
        |> Source.parse_copy_fields_config()

      payload = %{
        "food" => 123,
        "metadata" => %{"flat" => %{"existing_sibling" => 1}, "other" => "kept"}
      }

      assert %LogEvent{body: body} = LogEvent.make(payload, %{source: source})

      assert body["metadata"]["flat"]["field"] == 123
      assert body["metadata"]["flat"]["existing_sibling"] == 1
      assert body["metadata"]["other"] == "kept"
    end

    test "creates deeply nested intermediates when missing", %{source: source} do
      source =
        %{source | transform_copy_fields: "food:a.b.c.d"}
        |> Source.parse_copy_fields_config()

      assert %LogEvent{body: body} =
               LogEvent.make(%{"food" => 123}, %{source: source})

      assert body["a"]["b"]["c"]["d"] == 123
    end

    test "copies falsey-but-present source values", %{source: source} do
      source =
        %{source | transform_copy_fields: "flag:dest"}
        |> Source.parse_copy_fields_config()

      assert %LogEvent{body: body} =
               LogEvent.make(%{"flag" => false}, %{source: source})

      assert body["flag"] == false
      assert body["dest"] == false
    end
  end

  describe "kv_enrich" do
    setup %{source: source, user: user} do
      insert(:key_value,
        user: user,
        key: "123abc",
        value: %{"org_id" => "456def", "name" => "Acme"}
      )

      insert(:key_value,
        user: user,
        key: "xyz789",
        value: %{"result" => "enriched_val", "extra" => "data"}
      )

      insert(:key_value,
        user: user,
        key: "42",
        value: %{"result" => "found_it"}
      )

      insert(:key_value,
        user: user,
        key: "nested_proj",
        value: %{"org" => %{"id" => 123, "name" => "Acme"}, "role" => "admin"}
      )

      [source: source, user: user]
    end

    test "nil config is a no-op", %{source: source} do
      assert %LogEvent{body: body} =
               LogEvent.make(%{"project" => "abc"}, %{
                 source: %{source | transform_key_values: nil, transform_key_values_parsed: nil}
               })

      assert body["project"] == "abc"
      refute Map.has_key?(body, "enriched")
    end

    test "empty string config is a no-op", %{source: source} do
      source = %{source | transform_key_values: "", transform_key_values_parsed: nil}

      assert %LogEvent{body: body} =
               LogEvent.make(%{"project" => "abc"}, %{source: source})

      assert body["project"] == "abc"
    end

    test "whitespace-only config is a no-op", %{source: source} do
      source =
        %{source | transform_key_values: "   \n\t\n   "}
        |> Source.parse_key_values_config()

      assert source.transform_key_values_parsed == []

      assert %LogEvent{body: body} =
               LogEvent.make(%{"project" => "abc"}, %{source: source})

      assert body["project"] == "abc"
    end

    test "2-part pattern sets entire map at destination (pre-parsed)", %{source: source} do
      source =
        %{source | transform_key_values: "project:enriched"}
        |> Source.parse_key_values_config()

      assert %LogEvent{body: body} =
               LogEvent.make(%{"project" => "123abc"}, %{source: source})

      assert body["enriched"] == %{"org_id" => "456def", "name" => "Acme"}
    end

    test "2-part pattern works with unparsed fallback", %{source: source} do
      source = %{source | transform_key_values: "project:enriched"}

      assert %LogEvent{body: body} =
               LogEvent.make(%{"project" => "123abc"}, %{source: source})

      assert body["enriched"] == %{"org_id" => "456def", "name" => "Acme"}
    end

    test "preserves existing sibling keys at nested destination", %{source: source} do
      source =
        %{source | transform_key_values: "project:m.flat.lookup"}
        |> Source.parse_key_values_config()

      payload = %{
        "project" => "123abc",
        "metadata" => %{"flat" => %{"existing_sibling" => 1}, "other" => "kept"}
      }

      assert %LogEvent{body: body} = LogEvent.make(payload, %{source: source})

      assert body["metadata"]["flat"]["lookup"] == %{"org_id" => "456def", "name" => "Acme"}
      assert body["metadata"]["flat"]["existing_sibling"] == 1
      assert body["metadata"]["other"] == "kept"
    end

    test "3-part pattern with dot syntax accessor", %{source: source} do
      source =
        %{source | transform_key_values: "project:org_id:org_id"}
        |> Source.parse_key_values_config()

      assert %LogEvent{body: body} =
               LogEvent.make(%{"project" => "123abc"}, %{source: source})

      assert body["org_id"] == "456def"
    end

    test "3-part pattern with nested dot syntax accessor", %{source: source} do
      source =
        %{source | transform_key_values: "project:m.org_id:org.id"}
        |> Source.parse_key_values_config()

      assert %LogEvent{body: body} =
               LogEvent.make(%{"project" => "nested_proj"}, %{source: source})

      assert body["metadata"]["org_id"] == 123
    end

    test "3-part pattern with jsonpath accessor", %{source: source} do
      source =
        %{source | transform_key_values: "project:m.org_name:$.org.name"}
        |> Source.parse_key_values_config()

      assert %LogEvent{body: body} =
               LogEvent.make(%{"project" => "nested_proj"}, %{source: source})

      assert body["metadata"]["org_name"] == "Acme"
    end

    test "accessor path on missing nested key returns nil (no enrichment)", %{source: source} do
      source =
        %{source | transform_key_values: "project:result:nonexistent.path"}
        |> Source.parse_key_values_config()

      assert %LogEvent{body: body} =
               LogEvent.make(%{"project" => "123abc"}, %{source: source})

      refute Map.has_key?(body, "result")
    end

    test "multiple rules with mixed accessor paths", %{source: source} do
      source =
        %{source | transform_key_values: "project:org_id:org_id\ncode:extra:extra"}
        |> Source.parse_key_values_config()

      assert %LogEvent{body: body} =
               LogEvent.make(%{"project" => "123abc", "code" => "xyz789"}, %{source: source})

      assert body["org_id"] == "456def"
      assert body["extra"] == "data"
    end

    test "no match leaves event unchanged", %{source: source} do
      source =
        %{source | transform_key_values: "project:org_id:org_id"}
        |> Source.parse_key_values_config()

      assert %LogEvent{body: body} =
               LogEvent.make(%{"project" => "no_match"}, %{source: source})

      refute Map.has_key?(body, "org_id")
    end

    test "nested source paths with m. shorthand", %{source: source} do
      source =
        %{source | transform_key_values: "m.project_id:m.org_id:org_id"}
        |> Source.parse_key_values_config()

      assert %LogEvent{body: body} =
               LogEvent.make(%{"metadata" => %{"project_id" => "123abc"}}, %{source: source})

      assert body["metadata"]["org_id"] == "456def"
    end

    test "non-string field values are stringified for key lookup", %{source: source} do
      source =
        %{source | transform_key_values: "count:result:result"}
        |> Source.parse_key_values_config()

      assert %LogEvent{body: body} =
               LogEvent.make(%{"count" => 42}, %{source: source})

      assert body["result"] == "found_it"
    end
  end

  describe "drop_fields" do
    test "nil config is a no-op", %{source: source} do
      source = %{source | transform_drop_fields: nil}

      assert %LogEvent{body: body} =
               LogEvent.make(%{"keep" => 1, "service" => "router"}, %{source: source})

      assert body["keep"] == 1
      assert body["service"] == "router"
    end

    test "drops a top-level field", %{source: source} do
      source =
        %{source | transform_drop_fields: "service"}
        |> Source.parse_drop_fields_config()

      assert %LogEvent{body: body} =
               LogEvent.make(%{"service" => "router", "keep" => 1}, %{source: source})

      refute Map.has_key?(body, "service")
      assert body["keep"] == 1
    end

    test "works with unparsed fallback", %{source: source} do
      source = %{source | transform_drop_fields: "service"}

      assert %LogEvent{body: body} =
               LogEvent.make(%{"service" => "router", "keep" => 1}, %{source: source})

      refute Map.has_key?(body, "service")
      assert body["keep"] == 1
    end

    test "parsed virtual takes precedence over raw config string", %{source: source} do
      source = %{
        source
        | transform_drop_fields: "noop",
          transform_drop_fields_parsed: [["service"]]
      }

      assert %LogEvent{body: body} =
               LogEvent.make(
                 %{"service" => "router", "noop" => 1, "keep" => 2},
                 %{source: source}
               )

      refute Map.has_key?(body, "service")
      assert body["noop"] == 1
      assert body["keep"] == 2
    end

    test "drops a nested field via dot syntax", %{source: source} do
      source =
        %{source | transform_drop_fields: "metadata.user.id"}
        |> Source.parse_drop_fields_config()

      assert %LogEvent{body: body} =
               LogEvent.make(
                 %{"metadata" => %{"user" => %{"id" => 42, "name" => "ada"}}},
                 %{source: source}
               )

      assert body["metadata"]["user"] == %{"name" => "ada"}
    end

    test "m. shorthand resolves to metadata.", %{source: source} do
      source =
        %{source | transform_drop_fields: "m.routing.region"}
        |> Source.parse_drop_fields_config()

      assert %LogEvent{body: body} =
               LogEvent.make(
                 %{"metadata" => %{"routing" => %{"region" => "us-east"}, "kept" => 1}},
                 %{source: source}
               )

      refute get_in(body, ["metadata", "routing", "region"])
      assert body["metadata"]["kept"] == 1
    end

    test "drops multiple fields", %{source: source} do
      source =
        %{source | transform_drop_fields: "service\nnamespace\nm.routing.region"}
        |> Source.parse_drop_fields_config()

      assert %LogEvent{body: body} =
               LogEvent.make(
                 %{
                   "service" => "router",
                   "namespace" => "default",
                   "metadata" => %{"routing" => %{"region" => "us-east"}},
                   "keep" => 1
                 },
                 %{source: source}
               )

      refute Map.has_key?(body, "service")
      refute Map.has_key?(body, "namespace")
      refute get_in(body, ["metadata", "routing", "region"])
      assert body["keep"] == 1
    end

    test "missing fields are a silent no-op", %{source: source} do
      source =
        %{source | transform_drop_fields: "absent\nmetadata.also.absent"}
        |> Source.parse_drop_fields_config()

      assert %LogEvent{body: body} =
               LogEvent.make(%{"keep" => 1}, %{source: source})

      assert body["keep"] == 1
    end

    test "blank and whitespace-only lines are ignored", %{source: source} do
      source =
        %{source | transform_drop_fields: "\n  \nservice\n\n"}
        |> Source.parse_drop_fields_config()

      assert %LogEvent{body: body} =
               LogEvent.make(%{"service" => "router", "keep" => 1}, %{source: source})

      refute Map.has_key?(body, "service")
      assert body["keep"] == 1
    end

    test "whitespace-only config is a no-op", %{source: source} do
      source =
        %{source | transform_drop_fields: "   \n\t\n   "}
        |> Source.parse_drop_fields_config()

      assert source.transform_drop_fields_parsed == []

      assert %LogEvent{body: body} =
               LogEvent.make(%{"service" => "router", "keep" => 1}, %{source: source})

      assert body["service"] == "router"
      assert body["keep"] == 1
    end

    test "non-map intermediate path is a silent no-op", %{source: source} do
      source =
        %{source | transform_drop_fields: "service.region"}
        |> Source.parse_drop_fields_config()

      assert %LogEvent{body: body} =
               LogEvent.make(%{"service" => "router"}, %{source: source})

      assert body["service"] == "router"
    end

    test "reserved top-level fields are filtered at parse time", %{source: source} do
      source =
        %{source | transform_drop_fields: "id\nevent_message\ntimestamp\nservice"}
        |> Source.parse_drop_fields_config()

      assert source.transform_drop_fields_parsed == [["service"]]
    end

    test "reserved field names under metadata are still droppable", %{source: source} do
      source =
        %{source | transform_drop_fields: "metadata.id\nm.timestamp"}
        |> Source.parse_drop_fields_config()

      assert source.transform_drop_fields_parsed == [
               ["metadata", "id"],
               ["metadata", "timestamp"]
             ]
    end

    test "runs after copy_fields so users can copy then drop the source", %{source: source} do
      source =
        %{
          source
          | transform_copy_fields: "service:m.routing.service",
            transform_drop_fields: "service"
        }
        |> Source.parse_copy_fields_config()
        |> Source.parse_drop_fields_config()

      assert %LogEvent{body: body} =
               LogEvent.make(%{"service" => "router"}, %{source: source})

      refute Map.has_key?(body, "service")
      assert body["metadata"]["routing"]["service"] == "router"
    end

    test "runs after kv_enrich so users can enrich then drop the source", %{
      source: source,
      user: user
    } do
      insert(:key_value, user: user, key: "router", value: %{"org_id" => "acme"})

      source =
        %{
          source
          | transform_key_values: "service:enriched",
            transform_drop_fields: "service"
        }
        |> Source.parse_key_values_config()
        |> Source.parse_drop_fields_config()

      assert %LogEvent{body: body} =
               LogEvent.make(%{"service" => "router"}, %{source: source})

      refute Map.has_key?(body, "service")
      assert body["enriched"] == %{"org_id" => "acme"}
    end

    test "paths match post-bigquery_spec field names", %{source: source} do
      source =
        %{source | transform_drop_fields: "my-key"}
        |> Source.parse_drop_fields_config()

      assert %LogEvent{body: body} =
               LogEvent.make(%{"my-key" => 1, "keep" => 2}, %{source: source})

      assert body["_my_key"] == 1
      assert body["keep"] == 2

      source =
        %{source | transform_drop_fields: "_my_key"}
        |> Source.parse_drop_fields_config()

      assert %LogEvent{body: body} =
               LogEvent.make(%{"my-key" => 1, "keep" => 2}, %{source: source})

      refute Map.has_key?(body, "_my_key")
      assert body["keep"] == 2
    end
  end

  describe "transform pipeline" do
    test "runs all four stages in documented order", %{source: source, user: user} do
      insert(:key_value, user: user, key: "router", value: %{"org_id" => "acme"})

      source =
        %{
          source
          | transform_copy_fields: "_my_key:metadata.original",
            transform_key_values: "_my_key:enriched",
            transform_drop_fields: "_my_key"
        }
        |> Source.parse_copy_fields_config()
        |> Source.parse_key_values_config()
        |> Source.parse_drop_fields_config()

      assert %LogEvent{body: body} =
               LogEvent.make(%{"my-key" => "router", "extra" => "data"}, %{source: source})

      assert body["metadata"]["original"] == "router"
      assert body["enriched"] == %{"org_id" => "acme"}
      refute Map.has_key?(body, "_my_key")
      refute Map.has_key?(body, "my-key")
      assert body["extra"] == "data"
    end
  end

  test "make/2 with metadata string", %{source: source} do
    assert %LogEvent{
             body: body,
             drop: false,
             id: id,
             ingested_at: _,
             is_from_stale_query: nil,
             valid: true,
             pipeline_error: nil,
             via_rule_id: nil
           } = LogEvent.make(%{"metadata" => "some string"}, %{source: source})

    assert id == body["id"]
    assert body["metadata"] == "some string"
  end

  test "make/2 cast custom param values", %{source: source} do
    params =
      Map.merge(@vallog_event_ids, %{
        "valid" => false,
        "pipeline_error" => "some error"
      })

    assert %LogEvent{
             drop: false,
             # validity gets overwritten
             valid: true,
             pipeline_error: nil,
             source_id: source_id
           } = LogEvent.make(params, %{source: source})

    assert source_id == source.id
  end

  test "make_from_db/2", %{source: source} do
    params = %{"metadata" => []}
    assert %{body: body} = LogEvent.make_from_db(params, %{source: source})
    # metadata should be rejected
    assert body["metadata"] == nil

    params = %{"metadata" => [%{"some" => "value"}]}
    le = LogEvent.make_from_db(params, %{source: source})
    assert %{body: %{"metadata" => [%{"some" => "value"}]}, source_id: source_id} = le
    assert source_id == source.id
    assert le.body["event_message"] == nil
  end

  test "make_from_db/2 with string metadata", %{source: source} do
    params = %{"metadata" => "some string"}
    assert %{body: body} = LogEvent.make_from_db(params, %{source: source})
    assert body["metadata"] == "some string"
  end

  test "apply_custom_event_message/1 generates custom event message from source setting", %{
    source: source
  } do
    params = %{
      "event_message" => "some message",
      "metadata" => %{"a" => "value"}
    }

    source = %{source | custom_event_message_keys: "id, event_message, m.a"}
    le = LogEvent.make(params, %{source: source})

    le = LogEvent.apply_custom_event_message(le, source)
    assert le.body["event_message"] =~ le.id
    assert le.body["event_message"] =~ "value"
    assert le.body["event_message"] =~ "some message"
    assert le.body["message"] == nil
  end

  describe "timestamp_inferred flag" do
    test "is true when no timestamp is provided", %{source: source} do
      le = LogEvent.make(%{"event_message" => "test"}, %{source: source})
      assert le.timestamp_inferred == true
    end

    test "is false when a valid ISO 8601 timestamp is provided", %{source: source} do
      params = Map.put(@vallog_event_ids, "timestamp", "2024-01-01T00:00:00Z")
      le = LogEvent.make(params, %{source: source})
      assert le.timestamp_inferred == false
    end

    test "is true when an unparsable string timestamp is provided", %{source: source} do
      params = Map.put(@vallog_event_ids, "timestamp", "not-a-timestamp")
      le = LogEvent.make(params, %{source: source})
      assert le.timestamp_inferred == true
    end

    test "is false when a valid integer timestamp is provided", %{source: source} do
      params = Map.put(@vallog_event_ids, "timestamp", 1_713_268_565_764_892)
      le = LogEvent.make(params, %{source: source})
      assert le.timestamp_inferred == false
    end

    test "is false when a valid float timestamp is provided", %{source: source} do
      params = Map.put(@vallog_event_ids, "timestamp", 1_713_268_565.5)
      le = LogEvent.make(params, %{source: source})
      assert le.timestamp_inferred == false
    end

    test "is true when timestamp is an unsupported type", %{source: source} do
      params = Map.put(@vallog_event_ids, "timestamp", [1, 2, 3])
      le = LogEvent.make(params, %{source: source})
      assert le.timestamp_inferred == true
    end

    test "is true when timestamp is a negative integer", %{source: source} do
      params = Map.put(@vallog_event_ids, "timestamp", -1)
      le = LogEvent.make(params, %{source: source})
      assert le.timestamp_inferred == true
    end

    test "is true when timestamp is an empty string", %{source: source} do
      params = Map.put(@vallog_event_ids, "timestamp", "")
      le = LogEvent.make(params, %{source: source})
      assert le.timestamp_inferred == true
    end
  end

  describe "trace start_time substitution" do
    @one_hour_us 3_600_000_000

    test "replaces inferred timestamp with start_time for OTEL span events", %{source: source} do
      start_time_us = System.system_time(:microsecond) - @one_hour_us

      params = %{
        "event_message" => "trace with inferred timestamp",
        "metadata" => %{"type" => "span"},
        "start_time" => start_time_us
      }

      le = LogEvent.make(params, %{source: source})

      assert le.event_type == :trace
      assert le.timestamp_inferred == true
      assert le.body["timestamp"] == start_time_us
    end

    test "honors `start_time_unix_nano` (OTEL processor key)", %{source: source} do
      start_time_ns = System.system_time(:nanosecond) - 1_000_000_000

      params = %{
        "event_message" => "trace from otel processor",
        "metadata" => %{"type" => "span"},
        "start_time_unix_nano" => start_time_ns
      }

      le = LogEvent.make(params, %{source: source})

      assert le.body["timestamp"] == Utils.to_microseconds(start_time_ns)
    end

    test "does not override an explicit timestamp for trace events", %{source: source} do
      explicit_us = 1_700_000_000_000_000
      start_time_us = explicit_us - @one_hour_us

      params = %{
        "event_message" => "trace with explicit timestamp",
        "metadata" => %{"type" => "span"},
        "timestamp" => explicit_us,
        "start_time" => start_time_us
      }

      le = LogEvent.make(params, %{source: source})

      assert le.timestamp_inferred == false
      assert le.body["timestamp"] == explicit_us
    end

    test "does not substitute start_time for non-trace events", %{source: source} do
      start_time_us = System.system_time(:microsecond) - @one_hour_us

      params = %{
        "event_message" => "log with start_time field",
        "start_time" => start_time_us
      }

      le = LogEvent.make(params, %{source: source})

      assert le.event_type == :log
      refute le.body["timestamp"] == start_time_us
    end
  end

  describe "make_message/2" do
    property "if pattern is `nil` then the message is left as is" do
      check all message <- string(:printable) do
        le = event_with_message(nil, %{"message" => message})

        assert message == le.body["event_message"]
      end
    end

    test "message `id` is accessible" do
      le = event_with_message("id", %{})

      assert "#{le.id}" == le.body["event_message"]
    end

    test "pattern `message` and `event_message` can be used interchangeably" do
      for a <- ~w[message event_message],
          b <- ~w[message event_message] do
        message = "#{a} -> #{b}"
        le = event_with_message(a, %{b => message})

        assert message == le.body["event_message"]
      end
    end

    property "one can concat multiple fields" do
      check all message <- string(:printable) do
        le = event_with_message("id, message", %{"message" => message})

        assert "#{le.id} | #{message}" == le.body["event_message"]
      end
    end

    @alphas [?a..?z, ?A..?Z]

    property "`m.` can be used as an alias for `metadata.`" do
      check all key <- string(@alphas, min_length: 1),
                data <- string(:printable) do
        le1 = event_with_message("m.#{key}", %{"metadata" => %{key => data}})
        le2 = event_with_message("metadata.#{key}", le1)

        assert le1.body == le2.body
      end
    end

    property "top keys are reachable" do
      check all metadata <-
                  map_of(
                    string(@alphas, min_length: 1),
                    string(:printable),
                    min_length: 1
                  ) do
        key = Enum.random(Map.keys(metadata))

        le = event_with_message(key, metadata)

        assert Jason.encode!(metadata[key]) == le.body["event_message"]
      end
    end

    property "nested keys are reachable" do
      check all metadata <-
                  map_of(
                    string(@alphas, min_length: 2),
                    map_of(
                      string(@alphas, min_length: 1),
                      string(:printable, min_length: 1, max_length: 20),
                      min_length: 1,
                      max_length: 20
                    ),
                    min_length: 1,
                    max_length: 50
                  ) do
        first = Enum.random(Map.keys(metadata))
        second = Enum.random(Map.keys(metadata[first]))

        le = event_with_message("#{first}.#{second}", metadata)

        assert Jason.encode!(metadata[first][second]) == le.body["event_message"]
      end
    end

    property "timestamp unix conversions" do
      source = build(:source, user: build(:user))

      check all ts <-
                  one_of([
                    integer(1_713_268_565_764_892..1_713_268_565_764_999),
                    integer(1_713_268_565_764..1_713_268_565_999),
                    integer(1_713_268_565..1_713_268_999),
                    float(min: 1_713_268_565.1, max: 1_713_268_999.9),
                    float(min: 1_713_268_000_000.1, max: 1_713_268_000_999.9),
                    float(min: 1_713_268_565_000_000.1, max: 1_713_268_565_000_999.9)
                  ]) do
        params = Map.put(@vallog_event_ids, "timestamp", ts)
        LogEvent.make(params, %{source: source})
      end
    end

    defp event_with_message(pattern, %LogEvent{} = le) do
      @subject.apply_custom_event_message(
        le,
        %Source{custom_event_message_keys: pattern}
      )
    end

    defp event_with_message(pattern, %{} = body) do
      le = %LogEvent{
        id: Ecto.UUID.generate(),
        body: body
      }

      event_with_message(pattern, le)
    end
  end

  describe "make_from_spool/2" do
    test "preserves via_rule_id from an etf-decoded (atom-key) spool record", %{source: source} do
      record = %{
        id: Ecto.UUID.generate(),
        body: %{"message" => "hello"},
        event_type: :log,
        ingested_at: System.system_time(:microsecond),
        via_rule_id: 123
      }

      assert %LogEvent{via_rule_id: 123} = LogEvent.make_from_spool(record, source)
    end

    test "preserves via_rule_id from a json-decoded (string-key) spool record", %{source: source} do
      record = %{
        "id" => Ecto.UUID.generate(),
        "body" => %{"message" => "hello"},
        "event_type" => "log",
        "ingested_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
        "via_rule_id" => 123
      }

      assert %LogEvent{via_rule_id: 123} = LogEvent.make_from_spool(record, source)
    end

    test "defaults via_rule_id to nil for an etf-decoded record without the key (backward compat with older spool files)",
         %{source: source} do
      record = %{
        id: Ecto.UUID.generate(),
        body: %{"message" => "hello"},
        event_type: :log,
        ingested_at: System.system_time(:microsecond)
      }

      assert %LogEvent{via_rule_id: nil} = LogEvent.make_from_spool(record, source)
    end

    test "defaults via_rule_id to nil for a json-decoded record without the key (backward compat with older spool files)",
         %{source: source} do
      record = %{
        "id" => Ecto.UUID.generate(),
        "body" => %{"message" => "hello"},
        "event_type" => "log",
        "ingested_at" => DateTime.utc_now() |> DateTime.to_iso8601()
      }

      assert %LogEvent{via_rule_id: nil} = LogEvent.make_from_spool(record, source)
    end
  end
end
