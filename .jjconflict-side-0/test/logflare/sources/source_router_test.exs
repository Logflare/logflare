defmodule Logflare.Sources.SourceRouterTest do
  use Logflare.DataCase

  alias Logflare.Backends.SourceSup
  alias Logflare.LogEvent
  alias Logflare.Lql.Parser
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Rules.Rule
  alias Logflare.Sources.SourceRouter
  alias Logflare.SystemMetrics.AllLogsLogged

  @routers [SourceRouter.Sequential, SourceRouter.RulesTree]

  setup do
    start_supervised!(AllLogsLogged)
    insert(:plan)
    user = insert(:user)
    [user: user, backend: insert(:backend, user: user)]
  end

  for router <- @routers do
    describe "#{inspect(router)} handles" do
      test "SourceRouter LE handling", %{user: user, backend: backend} do
        rule = build(:rule, backend: backend, lql_string: "testing")
        source = insert(:source, user: user, rules: [rule])

        start_supervised!({SourceSup, source})

        %LogEvent{} = le = build(:log_event, source: source, message: "testing123")

        assert SourceRouter.route_to_sinks_and_ingest(le, source, unquote(router)) == %LogEvent{
                 le
                 | via_rule_id: rule.id
               }

        assert SourceRouter.route_to_sinks_and_ingest([le], source, unquote(router)) == [
                 %LogEvent{
                   le
                   | via_rule_id: rule.id
                 }
               ]
      end

      test "list_includes operator", %{user: user} do
        build_data = fn metadata_val, filter_val ->
          rule = %Rule{
            lql_string: "",
            lql_filters: [
              %FilterRule{
                value: filter_val,
                operator: :list_includes,
                modifiers: %{},
                path: "metadata.list_of_ints"
              }
            ]
          }

          source = insert(:source, user: user, rules: [rule])

          le =
            build(:log_event,
              source: source,
              metadata: %{"list_of_ints" => metadata_val}
            )

          {le, source}
        end

        {le, source} = build_data.([1, 2, 5, 0, -100, 1_000_000], 2)
        assert unquote(router).matching_rules(le, source) == source.rules

        {le, source} = build_data.([], 2)
        assert unquote(router).matching_rules(le, source) == []

        {le, source} = build_data.(["2", "6", "0"], nil)
        assert unquote(router).matching_rules(le, source) == []
      end

      test "list_includes operator on non-list", %{user: user} do
        build_data = fn rule_value, event_value ->
          lql_string = "user_agent:@>#{rule_value}"
          {:ok, filters} = Parser.parse(lql_string)
          rule = %Rule{lql_string: lql_string, lql_filters: filters}

          source = insert(:source, user: user, rules: [rule])

          le = LogEvent.make(%{"user_agent" => event_value}, %{source: source})

          {le, source}
        end

        {le, source} = build_data.("Chrome", "HomeAssistant/2026.2.2 aiohttp/3.13.3 Python/3.13")
        assert unquote(router).matching_rules(le, source) == []

        {le, source} = build_data.("Chrome", "Chrome")
        assert unquote(router).matching_rules(le, source) == source.rules
      end

      test "list_includes_regexp operator", %{user: user} do
        build_data = fn metadata_val, filter_val ->
          rule = %Rule{
            lql_string: "",
            lql_filters: [
              %FilterRule{
                value: filter_val,
                operator: :list_includes_regexp,
                modifiers: %{},
                path: "metadata.list"
              }
            ]
          }

          source = insert(:source, token: TestUtils.gen_uuid(), rules: [rule], user: user)

          le =
            build(:log_event,
              source: source,
              metadata: %{"list" => metadata_val}
            )

          {le, source}
        end

        {le, source} = build_data.(["a", "b", "abc123"], "23")
        assert unquote(router).matching_rules(le, source) == source.rules

        {le, source} = build_data.(["a", "b", "abc123"], "a\",")
        assert unquote(router).matching_rules(le, source) == []

        {le, source} = build_data.("a, b, abc123", "b,")
        assert unquote(router).matching_rules(le, source) == source.rules

        {le, source} = build_data.([], "23")
        assert unquote(router).matching_rules(le, source) == []
      end

      test "string_contains operator", %{user: user} do
        build_data = fn metadata_val, filter_val ->
          rule = %Rule{
            lql_string: "",
            lql_filters: [
              %FilterRule{
                value: filter_val,
                operator: :string_contains,
                modifiers: %{},
                path: "metadata.path"
              }
            ]
          }

          source = insert(:source, token: TestUtils.gen_uuid(), rules: [rule], user: user)

          le =
            build(:log_event,
              source: source,
              metadata: %{"path" => metadata_val}
            )

          {le, source}
        end

        {le, source} = build_data.("log error string", "error")
        assert unquote(router).matching_rules(le, source) == source.rules

        {le, source} = build_data.("log info string", "error")
        assert unquote(router).matching_rules(le, source) == []

        {le, source} = build_data.("stringstring", "string")
        assert unquote(router).matching_rules(le, source) == source.rules
      end

      test "string_contains operator 1", %{user: user} do
        build_data = fn message_val ->
          rule = %Rule{
            lql_string: "",
            lql_filters: [
              %FilterRule{
                modifiers: %{},
                operator: :string_contains,
                path: "event_message",
                shorthand: nil,
                value: "ten",
                values: nil
              }
            ]
          }

          source = insert(:source, token: TestUtils.gen_uuid(), rules: [rule], user: user)

          le =
            build(:log_event,
              source: source,
              message: message_val,
              metadata: %{"statusCode" => 200}
            )

          {le, source}
        end

        {le, source} = build_data.("ten three")
        assert unquote(router).matching_rules(le, source) == source.rules
      end

      test "regex match operator", %{user: user} do
        build_data = fn metadata_val, filter_val ->
          rule = %Rule{
            lql_string: "",
            lql_filters: [
              %FilterRule{
                value: filter_val,
                operator: :"~",
                modifiers: %{},
                path: "metadata.regex_string"
              }
            ]
          }

          source = insert(:source, token: TestUtils.gen_uuid(), rules: [rule], user: user)

          le =
            build(:log_event,
              source: source,
              metadata: %{"regex_string" => metadata_val}
            )

          {le, source}
        end

        {le, source} = build_data.("111", ~S|\d\d\d|)
        assert unquote(router).matching_rules(le, source) == source.rules

        {le, source} = build_data.("11z", ~S|\d\d\d|)
        assert unquote(router).matching_rules(le, source) == []
      end

      test "regex match on integer/float fields", %{user: user} do
        build_data = fn metadata_val, filter_val ->
          rule = %Rule{
            lql_string: "",
            lql_filters: [
              %FilterRule{
                value: filter_val,
                operator: :"~",
                modifiers: %{},
                path: "metadata.num"
              }
            ]
          }

          source = insert(:source, token: TestUtils.gen_uuid(), rules: [rule], user: user)

          le = build(:log_event, source: source, metadata: %{"num" => metadata_val})

          {le, source}
        end

        {le, source} = build_data.(123, "123")
        assert unquote(router).matching_rules(le, source) == source.rules
      end

      test "gt,lt,gte,lte operators", %{user: user} do
        build_data = fn metadata_val, filter_val, operator ->
          rule = %Rule{
            lql_string: "",
            lql_filters: [
              %FilterRule{
                value: filter_val,
                operator: operator,
                modifiers: %{},
                path: "metadata.number"
              }
            ]
          }

          source = insert(:source, token: TestUtils.gen_uuid(), rules: [rule], user: user)

          le =
            build(:log_event,
              source: source,
              metadata: %{"number" => metadata_val}
            )

          {le, source}
        end

        {le, source} = build_data.(100, 1, :>)
        assert unquote(router).matching_rules(le, source) == source.rules

        {le, source} = build_data.(100, 200, :<)
        assert unquote(router).matching_rules(le, source) == source.rules

        {le, source} = build_data.(1, 1, :>=)
        assert unquote(router).matching_rules(le, source) == source.rules

        {le, source} = build_data.(1, 1, :<=)
        assert unquote(router).matching_rules(le, source) == source.rules
      end

      test "multiple filters", %{user: user} do
        # TODO: variable for filter?
        build_data = fn field1_val, field2_val ->
          rule = %Rule{
            lql_string: "",
            lql_filters: [
              %FilterRule{
                value: 0,
                operator: :=,
                modifiers: %{},
                path: "metadata.field1"
              },
              %FilterRule{
                value: "string",
                operator: :"~",
                modifiers: %{},
                path: "metadata.field2"
              }
            ]
          }

          source = insert(:source, token: TestUtils.gen_uuid(), rules: [rule], user: user)

          le =
            build(:log_event,
              source: source,
              metadata: %{"field1" => field1_val, "field2" => field2_val}
            )

          {le, source}
        end

        {le, source} = build_data.(0, "string")
        assert unquote(router).matching_rules(le, source) == source.rules

        {le, source} = build_data.(1, "string")
        assert unquote(router).matching_rules(le, source) == []
      end

      test "multiple filters with negation", %{user: user} do
        build_data = fn field1_val, field2_val ->
          rule = %Rule{
            lql_string: "",
            lql_filters: [
              %FilterRule{
                value: 0,
                operator: :=,
                modifiers: %{negate: true},
                path: "metadata.field1"
              },
              %FilterRule{
                value: "string",
                operator: :"~",
                modifiers: %{},
                path: "metadata.field2"
              }
            ]
          }

          source = insert(:source, token: TestUtils.gen_uuid(), rules: [rule], user: user)

          le =
            build(:log_event,
              source: source,
              metadata: %{"field1" => field1_val, "field2" => field2_val}
            )

          {le, source}
        end

        {le, source} = build_data.(0, "string")
        assert unquote(router).matching_rules(le, source) == []

        {le, source} = build_data.(1, "string")
        assert unquote(router).matching_rules(le, source) == source.rules
      end

      test "multiple negated filter", %{user: user} do
        build_data = fn level_val ->
          rule = %Rule{
            lql_string: "",
            lql_filters: [
              %FilterRule{
                value: "info",
                operator: :=,
                modifiers: %{negate: true},
                path: "metadata.level"
              },
              %FilterRule{
                value: "error",
                operator: :=,
                modifiers: %{negate: true},
                path: "metadata.level"
              }
            ]
          }

          source = insert(:source, token: TestUtils.gen_uuid(), rules: [rule], user: user)

          le =
            build(:log_event,
              source: source,
              metadata: %{"level" => level_val}
            )

          {le, source}
        end

        {le, source} = build_data.("info")
        assert unquote(router).matching_rules(le, source) == []

        {le, source} = build_data.("error")
        assert unquote(router).matching_rules(le, source) == []

        {le, source} = build_data.("warn")
        assert unquote(router).matching_rules(le, source) == source.rules
      end

      test "nested lists with maps lvl4", %{user: user} do
        build_data = fn metadata_val, filter_val ->
          rule = %Rule{
            lql_string: "",
            lql_filters: [
              %FilterRule{
                value: filter_val,
                operator: :=,
                modifiers: %{},
                path: ~s|metadata.lines.data.field1.field2.field3|
              }
            ]
          }

          source = insert(:source, token: TestUtils.gen_uuid(), rules: [rule], user: user)

          le =
            build(:log_event,
              source: source,
              metadata: metadata_val
            )

          {le, source}
        end

        metadata = %{
          "lines" => [
            %{
              "data" => %{
                "field1" => [
                  %{
                    "field2" => [
                      %{
                        "field3" => "value"
                      }
                    ]
                  }
                ]
              }
            },
            %{
              "data" => %{
                "field1" => [
                  %{
                    "field2" => [
                      %{
                        "field3" => "other"
                      }
                    ]
                  }
                ]
              }
            }
          ]
        }

        {le, source} = build_data.(metadata, "value")
        assert unquote(router).matching_rules(le, source) == source.rules
      end

      test "nested lists with maps lvl1", %{user: user} do
        build_data = fn metadata_val, filter_val ->
          rule = %Rule{
            lql_string: "",
            lql_filters: [
              %FilterRule{
                value: filter_val,
                operator: :=,
                modifiers: %{},
                path: ~s|metadata.lines.data|
              }
            ]
          }

          source = insert(:source, token: TestUtils.gen_uuid(), rules: [rule], user: user)

          le =
            build(:log_event,
              source: source,
              metadata: metadata_val
            )

          {le, source}
        end

        metadata = %{
          "lines" => [
            %{"data" => "other_value"},
            %{"data" => "value"}
          ]
        }

        {le, source} = build_data.(metadata, "value")
        assert unquote(router).matching_rules(le, source) == source.rules
      end

      test "range operator", %{user: user} do
        build_data = fn metadata_val, lvalue, rvalue, modifiers ->
          rule = %Rule{
            lql_string: "",
            lql_filters: [
              %FilterRule{
                operator: :range,
                values: [lvalue, rvalue],
                modifiers: modifiers,
                path: "metadata.range"
              }
            ]
          }

          source = insert(:source, token: TestUtils.gen_uuid(), rules: [rule], user: user)

          le =
            build(:log_event,
              source: source,
              metadata: %{"range" => metadata_val}
            )

          {le, source}
        end

        {le, source} = build_data.(6, 1, 10, %{})
        assert unquote(router).matching_rules(le, source) == source.rules

        {le, source} = build_data.(6, 1, 10, %{negate: true})
        assert unquote(router).matching_rules(le, source) == []

        {le, source} = build_data.(100, 101, 500, %{})
        assert unquote(router).matching_rules(le, source) == []

        {le, source} = build_data.(100, 101, 500, %{negate: true})
        assert unquote(router).matching_rules(le, source) == source.rules
      end
    end

    describe "With nested lists #{router} handles" do
      test "list_includes operator", %{user: user} do
        build_data = fn metadata_val, filter_val ->
          rule = %Rule{
            lql_string: "",
            lql_filters: [
              %FilterRule{
                value: filter_val,
                operator: :list_includes,
                modifiers: %{},
                path: "metadata.level1.level2.list_of_ints"
              }
            ]
          }

          source = insert(:source, token: TestUtils.gen_uuid(), rules: [rule], user: user)

          le =
            build(:log_event,
              source: source,
              metadata: metadata_val
            )

          {le, source}
        end

        metadata = %{
          "level1" => [
            %{
              "level2" => [
                %{"list_of_ints" => [400, 300]},
                %{"list_of_ints" => [100, 200, 2]}
              ]
            },
            %{
              "level2" => [
                %{"list_of_ints" => [2]},
                %{"list_of_ints" => []},
                %{"list_of_ints" => [4]}
              ]
            }
          ]
        }

        {le, source} = build_data.(metadata, 2)
        assert unquote(router).matching_rules(le, source) == source.rules

        {le, source} = build_data.(metadata, 4)
        assert unquote(router).matching_rules(le, source) == source.rules

        {le, source} = build_data.(metadata, 400)
        assert unquote(router).matching_rules(le, source) == source.rules

        {le, source} = build_data.(metadata, 350)
        assert unquote(router).matching_rules(le, source) == []

        {le, source} = build_data.(metadata, nil)
        assert unquote(router).matching_rules(le, source) == []

        {le, source} = build_data.(metadata, "200")
        assert unquote(router).matching_rules(le, source) == []
      end

      test "multiple matches and multiple rules with lists", %{user: user} do
        build_data = fn metadata_val, filter_val ->
          rule = %Rule{
            lql_string: "",
            lql_filters: [
              %FilterRule{
                value: filter_val,
                operator: :=,
                modifiers: %{},
                path: "metadata.level1.int"
              },
              %FilterRule{
                value: "valid",
                operator: :=,
                modifiers: %{},
                path: "metadata.ref"
              }
            ]
          }

          source = insert(:source, token: TestUtils.gen_uuid(), rules: [rule], user: user)

          le =
            build(:log_event,
              source: source,
              metadata: metadata_val
            )

          {le, source}
        end

        metadata = %{
          "ref" => "invalid",
          "level1" => [
            %{"int" => 2},
            %{"int" => 300},
            %{"int" => 2}
          ]
        }

        {le, source} = build_data.(metadata, 2)
        assert unquote(router).matching_rules(le, source) == []

        metadata = %{metadata | "ref" => "valid"}
        {le, source} = build_data.(metadata, 2)
        assert unquote(router).matching_rules(le, source) == source.rules
      end

      test "eq operator with nested maps lvl4", %{user: user} do
        build_data = fn metadata_val, filter_val ->
          rule = %Rule{
            lql_string: "",
            lql_filters: [
              %FilterRule{
                value: filter_val,
                operator: :=,
                modifiers: %{},
                path: ~s|metadata.lines.data.field1.field2.field3|
              }
            ]
          }

          source = insert(:source, token: TestUtils.gen_uuid(), rules: [rule], user: user)

          le =
            build(:log_event,
              source: source,
              metadata: metadata_val
            )

          {le, source}
        end

        metadata = %{
          "lines" => [
            %{
              "data" => %{
                "field1" => [
                  %{
                    "field2" => [
                      %{
                        "field3" => "value"
                      }
                    ]
                  }
                ]
              }
            },
            %{
              "data" => %{
                "field1" => [
                  %{
                    "field2" => [
                      %{
                        "field3" => "other"
                      }
                    ]
                  }
                ]
              }
            }
          ]
        }

        {le, source} = build_data.(metadata, "value")
        assert unquote(router).matching_rules(le, source) == source.rules
      end

      test "eq operator with nested maps lvl1", %{user: user} do
        build_data = fn metadata_val, filter_val ->
          rule = %Rule{
            lql_string: "",
            lql_filters: [
              %FilterRule{
                value: filter_val,
                operator: :=,
                modifiers: %{},
                path: ~s|metadata.lines.data|
              }
            ]
          }

          source = insert(:source, token: TestUtils.gen_uuid(), rules: [rule], user: user)

          le =
            build(:log_event,
              source: source,
              metadata: metadata_val
            )

          {le, source}
        end

        metadata = %{
          "lines" => [
            %{"data" => "other_value"},
            %{"data" => "value"}
          ]
        }

        {le, source} = build_data.(metadata, "value")
        assert unquote(router).matching_rules(le, source) == source.rules
      end
    end
  end
end
