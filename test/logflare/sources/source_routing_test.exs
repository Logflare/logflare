defmodule Logflare.Logs.SourceRoutingTest do
  use Logflare.DataCase

  alias Logflare.Logs.SourceRouting
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Rules.Rule

  describe "Source Routing LQL operator rules" do
    test "list_includes operator" do
      source = build(:source, token: TestUtils.gen_uuid(), rules: [])

      build_filter = fn value ->
        %Rule{
          lql_string: "",
          lql_filters: [
            %FilterRule{
              value: value,
              operator: :list_includes,
              modifiers: %{},
              path: "metadata.list_of_ints"
            }
          ]
        }
      end

      build_le = fn value ->
        build(:log_event,
          source: source,
          metadata: %{"list_of_ints" => value}
        )
      end

      le = build_le.([1, 2, 5, 0, -100, 1_000_000])
      rule = build_filter.(2)

      assert SourceRouting.route_with_lql_rules?(le, rule)

      le = build_le.([])
      rule = build_filter.(2)

      refute SourceRouting.route_with_lql_rules?(le, rule)

      le = build_le.(["2", "6", "0"])
      rule = build_filter.(nil)

      refute SourceRouting.route_with_lql_rules?(le, rule)
    end

    test "list_includes_regexp operator" do
      source = build(:source, token: TestUtils.gen_uuid(), rules: [])

      build_filter = fn value ->
        %Rule{
          lql_string: "",
          lql_filters: [
            %FilterRule{
              value: value,
              operator: :list_includes_regexp,
              modifiers: %{},
              path: "metadata.list"
            }
          ]
        }
      end

      build_le = fn value ->
        build(:log_event,
          source: source,
          metadata: %{"list" => value}
        )
      end

      le = build_le.(["a", "b", "abc123"])
      rule = build_filter.("23")

      assert SourceRouting.route_with_lql_rules?(le, rule)

      le = build_le.([])
      rule = build_filter.("23")

      refute SourceRouting.route_with_lql_rules?(le, rule)
    end

    test "string_contains operator" do
      source = build(:source, token: TestUtils.gen_uuid(), rules: [])

      build_filter = fn value ->
        %Rule{
          lql_string: "",
          lql_filters: [
            %FilterRule{
              value: value,
              operator: :string_contains,
              modifiers: %{},
              path: "metadata.path"
            }
          ]
        }
      end

      build_le = fn value ->
        build(:log_event,
          source: source,
          metadata: %{"path" => value}
        )
      end

      le = build_le.("log error string")
      rule = build_filter.("error")

      assert SourceRouting.route_with_lql_rules?(le, rule)

      le = build_le.("log info string")
      rule = build_filter.("error")

      refute SourceRouting.route_with_lql_rules?(le, rule)

      le = build_le.("stringstring")
      rule = build_filter.("string")

      assert SourceRouting.route_with_lql_rules?(le, rule)
    end

    test "string_contains operator 1" do
      source = build(:source, token: TestUtils.gen_uuid(), rules: [])

      build_filter = fn _value ->
        %Rule{
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
      end

      rule = build_filter.(0)

      params = %{"event_message" => "ten three", "metadata" => %{"statusCode" => 200}}

      le = Logflare.LogEvent.make(params, %{source: source})

      assert SourceRouting.route_with_lql_rules?(le, rule)
    end

    test "regex match operator" do
      source = build(:source, token: TestUtils.gen_uuid(), rules: [])

      build_filter = fn value ->
        %Rule{
          lql_string: "",
          lql_filters: [
            %FilterRule{
              value: value,
              operator: :"~",
              modifiers: %{},
              path: "metadata.regex_string"
            }
          ]
        }
      end

      build_le = fn value ->
        build(:log_event,
          source: source,
          metadata: %{"regex_string" => value}
        )
      end

      le = build_le.("111")
      rule = build_filter.(~S|\d\d\d|)

      assert SourceRouting.route_with_lql_rules?(le, rule)

      le = build_le.("11z")
      rule = build_filter.(~S|\d\d\d|)

      refute SourceRouting.route_with_lql_rules?(le, rule)
    end

    test "regex match on integer/float fields" do
      rule = %Rule{
        lql_string: "",
        lql_filters: [
          %FilterRule{
            value: "123",
            operator: :"~",
            modifiers: %{},
            path: "metadata.num"
          }
        ]
      }

      le = build(:log_event, metadata: %{"num" => 123})

      assert SourceRouting.route_with_lql_rules?(le, rule)
    end

    test "gt,lt,gte,lte operators" do
      source = build(:source, token: TestUtils.gen_uuid(), rules: [])

      build_filter = fn value, operator ->
        %Rule{
          lql_string: "",
          lql_filters: [
            %FilterRule{
              value: value,
              operator: operator,
              modifiers: %{},
              path: "metadata.number"
            }
          ]
        }
      end

      build_le = fn value ->
        build(:log_event,
          source: source,
          metadata: %{"number" => value}
        )
      end

      le = build_le.(100)
      rule = build_filter.(1, :>)
      assert SourceRouting.route_with_lql_rules?(le, rule)

      le = build_le.(100)
      rule = build_filter.(200, :<)
      assert SourceRouting.route_with_lql_rules?(le, rule)

      le = build_le.(1)
      rule = build_filter.(1, :>=)
      assert SourceRouting.route_with_lql_rules?(le, rule)

      le = build_le.(1)
      rule = build_filter.(1, :<=)
      assert SourceRouting.route_with_lql_rules?(le, rule)
    end

    test "multiple filters" do
      source = build(:source, token: TestUtils.gen_uuid(), rules: [])

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

      le =
        build(:log_event,
          source: source,
          metadata: %{"field1" => 0, "field2" => "string"}
        )

      assert SourceRouting.route_with_lql_rules?(le, rule)

      le =
        build(:log_event,
          source: source,
          metadata: %{"field1" => 1, "field2" => "string"}
        )

      refute SourceRouting.route_with_lql_rules?(le, rule)
    end

    test "multiple filters with negation" do
      source = build(:source, token: TestUtils.gen_uuid(), rules: [])

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

      le =
        build(:log_event,
          source: source,
          metadata: %{"field1" => 0, "field2" => "string"}
        )

      refute SourceRouting.route_with_lql_rules?(le, rule)

      le =
        build(:log_event,
          source: source,
          metadata: %{"field1" => 1, "field2" => "string"}
        )

      assert SourceRouting.route_with_lql_rules?(le, rule)
    end

    test "multiple negated filter" do
      source = build(:source, token: TestUtils.gen_uuid(), rules: [])

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

      le =
        build(:log_event,
          source: source,
          metadata: %{"level" => "info"}
        )

      refute SourceRouting.route_with_lql_rules?(le, rule)

      le =
        build(:log_event,
          source: source,
          metadata: %{"level" => "error"}
        )

      refute SourceRouting.route_with_lql_rules?(le, rule)

      le =
        build(:log_event,
          source: source,
          metadata: %{"level" => "warn"}
        )

      assert SourceRouting.route_with_lql_rules?(le, rule)
    end

    test "nested lists with maps lvl4" do
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

      source = build(:source, token: TestUtils.gen_uuid(), rules: [])

      rule = %Rule{
        lql_string: "",
        lql_filters: [
          %FilterRule{
            value: "value",
            operator: :=,
            modifiers: %{},
            path: ~s|metadata.lines.data.field1.field2.field3|
          }
        ]
      }

      le =
        build(:log_event,
          source: source,
          metadata: metadata
        )

      assert SourceRouting.route_with_lql_rules?(le, rule)
    end

    test "nested lists with maps lvl1" do
      source = build(:source, token: TestUtils.gen_uuid(), rules: [])

      metadata = %{
        "lines" => [
          %{"data" => "other_value"},
          %{"data" => "value"}
        ]
      }

      le =
        build(:log_event,
          source: source,
          metadata: metadata
        )

      rule = %Rule{
        lql_string: "",
        lql_filters: [
          %FilterRule{
            value: "value",
            operator: :=,
            modifiers: %{},
            path: ~s|metadata.lines.data|
          }
        ]
      }

      assert SourceRouting.route_with_lql_rules?(le, rule)
    end

    test "range operator" do
      source = build(:source, token: TestUtils.gen_uuid(), rules: [])

      build_filter = fn lvalue, rvalue, modifiers ->
        %Rule{
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
      end

      build_le = fn value ->
        build(:log_event,
          source: source,
          metadata: %{"range" => value}
        )
      end

      le = build_le.(6)
      rule = build_filter.(1, 10, %{})
      assert SourceRouting.route_with_lql_rules?(le, rule)

      rule = build_filter.(1, 10, %{negate: true})
      refute SourceRouting.route_with_lql_rules?(le, rule)

      le = build_le.(100)
      rule = build_filter.(101, 500, %{})

      refute SourceRouting.route_with_lql_rules?(le, rule)

      rule = build_filter.(101, 500, %{negate: true})
      assert SourceRouting.route_with_lql_rules?(le, rule)
    end
  end

  describe "Source Routing LQL with nested lists" do
    test "list_includes operator" do
      source = build(:source, token: TestUtils.gen_uuid(), rules: [])

      build_filter = fn value ->
        %Rule{
          lql_string: "",
          lql_filters: [
            %FilterRule{
              value: value,
              operator: :list_includes,
              modifiers: %{},
              path: "metadata.level1.level2.list_of_ints"
            }
          ]
        }
      end

      le =
        build(:log_event,
          source: source,
          metadata: %{
            "level1" => [
              %{
                "level2" => [%{"list_of_ints" => [400, 300]}, %{"list_of_ints" => [100, 200, 2]}]
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
        )

      rule = build_filter.(2)
      assert SourceRouting.route_with_lql_rules?(le, rule)

      rule = build_filter.(4)
      assert SourceRouting.route_with_lql_rules?(le, rule)

      rule = build_filter.(400)
      assert SourceRouting.route_with_lql_rules?(le, rule)

      rule = build_filter.(350)
      refute SourceRouting.route_with_lql_rules?(le, rule)

      rule = build_filter.(nil)
      refute SourceRouting.route_with_lql_rules?(le, rule)

      rule = build_filter.("200")
      refute SourceRouting.route_with_lql_rules?(le, rule)
    end

    test "eq operator with nested maps lvl4" do
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

      source = build(:source, token: TestUtils.gen_uuid(), rules: [])

      rule = %Rule{
        lql_string: "",
        lql_filters: [
          %FilterRule{
            value: "value",
            operator: :=,
            modifiers: %{},
            path: ~s|metadata.lines.data.field1.field2.field3|
          }
        ]
      }

      le =
        build(:log_event,
          source: source,
          metadata: metadata
        )

      assert SourceRouting.route_with_lql_rules?(le, rule)
    end

    test "eq operator with nested maps lvl1" do
      source = build(:source, token: TestUtils.gen_uuid(), rules: [])

      metadata = %{
        "lines" => [
          %{"data" => "other_value"},
          %{"data" => "value"}
        ]
      }

      le =
        build(:log_event,
          source: source,
          metadata: metadata
        )

      rule = %Rule{
        lql_string: "",
        lql_filters: [
          %FilterRule{
            value: "value",
            operator: :=,
            modifiers: %{},
            path: ~s|metadata.lines.data|
          }
        ]
      }

      assert SourceRouting.route_with_lql_rules?(le, rule)
    end
  end
end
