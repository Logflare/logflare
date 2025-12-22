defmodule Logflare.Logs.GroupedSourceRoutingTest do
  alias Logflare.Logs.GroupedSourceRouting
  use Logflare.DataCase

  alias Logflare.Logs.SourceRouting
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Rules.Rule

  test "matching" do
    rules = [
      %Rule{
        id: 0,
        lql_string: "",
        lql_filters: [
          %FilterRule{
            value: 0,
            operator: :=,
            modifiers: %{},
            path: "metadata.field1"
          }
        ],
        sink: :a
      },
      %Rule{
        id: 1,
        lql_string: "",
        lql_filters: [
          %FilterRule{
            value: "sth",
            operator: :=,
            modifiers: %{},
            path: "metadata.field2"
          }
        ],
        sink: :b
      }
    ]

    le =
      build(:log_event,
        body: "Log 0: Info phase: finish",
        event_message: "Log 0: Info phase: finish",
        metadata: %{"field1" => "42", "field2" => "sth"},
        timestamp: 1_765_540_359_181_556
      )

    rule_set = RuleSet.make(rules)
    assert GroupedSourceRouting.matching_rules(le, rule_set) == [1]
  end

  test "WIP" do
    rules = [
      %Rule{
        id: 0,
        lql_string: "",
        lql_filters: [
          %FilterRule{
            value: 0,
            operator: :=,
            modifiers: %{},
            path: "metadata.field1"
          }
        ],
        sink: :a
      },
      %Rule{
        id: 1,
        lql_string: "",
        lql_filters: [
          %FilterRule{
            value: "sth",
            operator: :=,
            modifiers: %{},
            path: "metadata.field2"
          }
        ],
        sink: :b
      }
    ]

    assert RuleSet.make(rules) ==
             %{
               {:get, "metadata"} => %{
                 {:get, "field1"} => %{{:equal, 0} => {:route, 0}},
                 {:get, "field2"} => %{{:equal, "sth"} => {:route, 1}}
               }
             }
  end

  test "WIP field conflict" do
    rules = [
      %Rule{
        id: 0,
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
          },
          %FilterRule{
            value: 0,
            operator: :=,
            modifiers: %{},
            path: "metadata.field3"
          }
        ],
        sink: :a
      },
      %Rule{
        id: 1,
        lql_string: "",
        lql_filters: [
          %FilterRule{
            value: "sth",
            operator: :=,
            modifiers: %{},
            path: "metadata.field1"
          },
          %FilterRule{
            value: "debug",
            operator: :=,
            modifiers: %{},
            path: "metadata.field2.subfield"
          },
          %FilterRule{
            value: 0,
            operator: :=,
            modifiers: %{},
            path: "metadata.field3"
          }
        ],
        sink: :b
      }
    ]

    assert RuleSet.make(rules) ==
             %{
               {:get, "metadata"} => %{
                 {:get, "field1"} => %{
                   {:equal, 0} => {:route, 0},
                   {:equal, "sth"} => {:route, 1}
                 },
                 {:get, "field2"} => %{
                   {:match, "string"} => {:route, 0},
                   {:get, "subfield"} => %{{:equal, "debug"} => {:route, 1}}
                 },
                 {:get, "field3"} => %{
                   {:equal, 0} => {:route, [0, 1]}
                 }
               }
             }
  end
end
