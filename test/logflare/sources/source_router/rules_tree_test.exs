defmodule Logflare.Sources.SourceRouter.RulesTreeTest do
  use Logflare.DataCase

  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Rules.Rule
  alias Logflare.Sources.SourceRouter.RulesTree

  @subject RulesTree

  describe "Build RulesTree for" do
    setup do
      [user: insert(:user)]
    end

    test "Two simple rules" do
      rules = [
        %Rule{
          id: 0,
          lql_filters: [
            %FilterRule{
              value: 0,
              operator: :=,
              path: "metadata.field1"
            }
          ]
        },
        %Rule{
          id: 1,
          lql_filters: [
            %FilterRule{
              value: "sth",
              operator: :=,
              path: "metadata.field2"
            }
          ]
        }
      ]

      assert @subject.build(rules) ==
               [
                 {"metadata",
                  [
                    {"field1", [{{:=, 0}, {:route, {0, 0b0}}}]},
                    {"field2", [{{:=, "sth"}, {:route, {1, 0b0}}}]}
                  ]}
               ]
    end

    test "rules with the same filters" do
      filter = %FilterRule{
        value: 0,
        operator: :=,
        path: "metadata.field1"
      }

      rules = [%Rule{id: 0, lql_filters: [filter]}, %Rule{id: 1, lql_filters: [filter]}]

      assert [
               {"metadata",
                [
                  {"field1",
                   [
                     {
                       {:=, 0},
                       {:route, targets}
                     }
                   ]}
                ]}
             ] = @subject.build(rules)

      assert {0, 0b0} in targets
      assert {1, 0b0} in targets
    end

    test "rules with similar filters" do
      filter = %FilterRule{
        value: 1,
        operator: :=,
        path: "metadata.field1"
      }

      rules = [
        %Rule{id: 0, lql_filters: [%{filter | value: 0}]},
        %Rule{id: 1, lql_filters: [filter]}
      ]

      assert [
               {"metadata",
                [
                  {"field1",
                   [
                     {{:=, 0}, {:route, {0, 0b0}}},
                     {{:=, 1}, {:route, {1, 0b0}}}
                   ]}
                ]}
             ] = @subject.build(rules)
    end
  end

  test "filters registry" do
    assert @subject.build_filter_flagset(1) == 0b1
    assert @subject.build_filter_flagset(2) == 0b11
    assert @subject.build_filter_flagset(5) == 0b11_111

    assert @subject.build_filter_bitmask(1, 0) == 0b0
    assert @subject.build_filter_bitmask(4, 0) == 0b1110
    assert @subject.build_filter_bitmask(4, 2) == 0b1011

    assert (set = @subject.build_filter_flagset(4)) == 0b1111

    assert (set = @subject.apply_filter_bitmask(set, @subject.build_filter_bitmask(4, 0))) ==
             0b1110

    assert (set = @subject.apply_filter_bitmask(set, @subject.build_filter_bitmask(4, 3))) ==
             0b0110

    assert (set = @subject.apply_filter_bitmask(set, @subject.build_filter_bitmask(4, 1))) ==
             0b0100

    assert (set = @subject.apply_filter_bitmask(set, @subject.build_filter_bitmask(4, 1))) ==
             0b0100

    assert (set = @subject.apply_filter_bitmask(set, @subject.build_filter_bitmask(4, 2))) ==
             0b0000

    assert set == 0
  end
end
