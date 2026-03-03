defmodule Logflare.LogEventTest do
  @moduledoc false
  use Logflare.DataCase
  use ExUnitProperties

  alias Logflare.LogEvent
  alias Logflare.Sources.Source

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

  describe "make/2 transformations" do
    test "dashes to underscores", %{source: source} do
      assert %LogEvent{
               body: %{
                 "_test_field" => _
               }
             } = LogEvent.make(%{"test-field" => 123}, %{source: source})
    end

    test "field copying - nested", %{source: source} do
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

    test "field copying - top level", %{source: source} do
      source = %{
        source
        | transform_copy_fields: """
            food:field
          """
      }

      assert %LogEvent{
               body: %{
                 "field" => 123,
                 "food" => 123
               }
             } = LogEvent.make(%{"food" => 123}, %{source: source})
    end

    test "field copying - multiple", %{source: source} do
      source = %{
        source
        | transform_copy_fields: """
            food:field
            field:123
          """
      }

      assert %LogEvent{
               body: %{
                 "123" => 123,
                 "field" => 123,
                 "food" => 123
               }
             } = LogEvent.make(%{"food" => 123}, %{source: source})
    end

    test "field copying - dashes in field", %{source: source} do
      source = %{
        source
        | transform_copy_fields: """
            _my_food:field
          """
      }

      assert %LogEvent{body: body} = LogEvent.make(%{"my-food" => 123}, %{source: source})
      assert Map.drop(body, ["id", "timestamp"]) == %{"_my_food" => 123, "field" => 123}
    end

    test "field copying - invalid instructions are ignored", %{source: source} do
      source = %{
        source
        | transform_copy_fields: """
            food field
            food-field
            foodfield
            missing:field
          """
      }

      assert %LogEvent{body: body} = LogEvent.make(%{"food" => 123}, %{source: source})
      assert Map.drop(body, ["id", "timestamp"]) == %{"food" => 123}
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
end
