defmodule Logflare.Sources.SourceRouter.RulesTreeTest do
  use Logflare.DataCase

  alias Logflare.LogEvent
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Rules.Rule
  alias Logflare.Sources.SourceRouter.RulesTree

  @subject RulesTree

  describe "Build RulesTree for" do
    setup do
      [user: insert(:user)]
    end

    test "No rules" do
      assert @subject.build([]) == []
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
                    {"field1", [{:eq_index, %{0 => 0}}]},
                    {"field2", [{:eq_index, %{"sth" => 1}}]}
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
                     {:eq_index, %{0 => targets}}
                   ]}
                ]}
             ] = @subject.build(rules)

      assert 0 in targets
      assert 1 in targets
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
                     {:eq_index, %{0 => 0, 1 => 1}}
                   ]}
                ]}
             ] = @subject.build(rules)
    end

    test "Multi-filter rule" do
      rules = [
        %Rule{
          id: 0,
          lql_filters: [
            %FilterRule{value: "foo", operator: :=, path: "project"},
            %FilterRule{value: "error", operator: :=, path: "level"}
          ]
        }
      ]

      # Multi-filter rules keep the tuple target so the bitmask accumulates.
      # filter 0 (project): bitmask = bxor(0b11, 0b01) = 0b10
      # filter 1 (level):   bitmask = bxor(0b11, 0b10) = 0b01
      assert @subject.build(rules) ==
               [
                 {"level", [{:eq_index, %{"error" => {0, 0b01}}}]},
                 {"project", [{:eq_index, %{"foo" => {0, 0b10}}}]}
               ]
    end

    test "Negated single-filter rule" do
      rules = [
        %Rule{
          id: 0,
          lql_filters: [
            %FilterRule{
              value: "drop_me",
              operator: :=,
              path: "level",
              modifiers: %{negate: true}
            }
          ]
        }
      ]

      # Negated equality is NOT folded into eq_index — stays as a regular
      # operator leaf. Single-filter target is still a bare integer.
      assert @subject.build(rules) ==
               [{"level", [{{:not, {:=, "drop_me"}}, {:route, 0}}]}]
    end
  end

  describe "matching_rule_ids/2" do
    test "single positive equality matches when value present" do
      rules = [rule(0, [filter("project", :=, "foo")])]
      assert matching(rules, %{"project" => "foo"}) == [0]
    end

    test "single positive equality misses on differing value" do
      rules = [rule(0, [filter("project", :=, "foo")])]
      assert matching(rules, %{"project" => "bar"}) == []
    end

    test "single positive equality misses when key absent" do
      rules = [rule(0, [filter("project", :=, "foo")])]
      assert matching(rules, %{"other" => "foo"}) == []
    end

    test "multiple sibling positive equality leaves: exactly one matches" do
      rules = for i <- 0..4, do: rule(i, [filter("project", :=, "v#{i}")])
      assert matching(rules, %{"project" => "v3"}) == [3]
    end

    test "multiple sibling positive equality leaves: none match" do
      rules = for i <- 0..4, do: rule(i, [filter("project", :=, "v#{i}")])
      assert matching(rules, %{"project" => "miss"}) == []
    end

    test "positive equality coexists with regex, range, and negated equality on same path" do
      rules = [
        rule(0, [filter("level", :=, "info")]),
        rule(1, [filter("level", :=, "warn")]),
        rule(2, [filter("level", :"~", "err.*")]),
        rule(3, [range_filter("level", 1, 5)]),
        rule(4, [filter("level", :=, "drop_me", %{negate: true})]),
        # duplicate range — exercises route-list consolidation through a non-eq
        # leaf (bare-int targets walked via accumulate's list head clause)
        rule(5, [range_filter("level", 1, 5)])
      ]

      assert matching(rules, %{"level" => "info"}) == [0, 4]
      assert matching(rules, %{"level" => "warn"}) == [1, 4]
      assert matching(rules, %{"level" => "error_500"}) == [2, 4]
      assert matching(rules, %{"level" => 3}) == [3, 4, 5]
      assert matching(rules, %{"level" => "drop_me"}) == []
    end

    test "multi-filter rule alongside single-filter rules at same path" do
      rules = [
        rule(0, [filter("project", :=, "foo")]),
        # same value as rule 0 — homogeneous eq_index entry mixing bare-int + tuple
        rule(1, [filter("project", :=, "foo"), filter("level", :=, "error")]),
        # different value at same path — exercises heterogeneous eq_index map
        # where one key resolves to a bare int and another to a tuple
        rule(2, [filter("project", :=, "bar"), filter("level", :=, "error")]),
        # 3 filters — exercises bitmask accumulation through more than 2 paths
        rule(3, [
          filter("project", :=, "foo"),
          filter("level", :=, "info"),
          filter("type", :=, "trace")
        ])
      ]

      assert matching(rules, %{"project" => "foo", "level" => "error"}) == [0, 1]
      assert matching(rules, %{"project" => "foo", "level" => "info"}) == [0]
      assert matching(rules, %{"project" => "bar", "level" => "error"}) == [2]
      assert matching(rules, %{"project" => "bar", "level" => "info"}) == []
      assert matching(rules, %{"project" => "baz", "level" => "error"}) == []

      # 3-filter rule: all three match -> emitted alongside rule 0
      assert matching(rules, %{"project" => "foo", "level" => "info", "type" => "trace"}) ==
               [0, 3]

      # 3-filter rule: two of three match (type missing) -> not emitted
      assert matching(rules, %{"project" => "foo", "level" => "info", "type" => "log"}) == [0]
    end

    test "two rules with identical filter both match (route list consolidation)" do
      f = filter("project", :=, "foo")
      rules = [rule(0, [f]), rule(1, [f])]
      assert matching(rules, %{"project" => "foo"}) == [0, 1]
    end

    test "nested dotted path descends into maps" do
      rules = [rule(0, [filter("metadata.response.status", :=, 200)])]
      assert matching(rules, %{"metadata" => %{"response" => %{"status" => 200}}}) == [0]
      assert matching(rules, %{"metadata" => %{"response" => %{"status" => 500}}}) == []
    end

    test "lists nested in body are traversed for descendant keys" do
      rules = [rule(0, [filter("metadata.tag", :=, "x")])]
      assert matching(rules, %{"metadata" => [%{"tag" => "y"}, %{"tag" => "x"}]}) == [0]
      assert matching(rules, %{"metadata" => [%{"tag" => "y"}, %{"tag" => "z"}]}) == []

      # bare-int accumulate must be idempotent — a single-filter rule that matches
      # twice via list traversal still appears exactly once in the result
      assert matching(rules, %{"metadata" => [%{"tag" => "x"}, %{"tag" => "x"}]}) == [0]
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

  describe "Benchmark" do
    @describetag :benchmark
    @describetag timeout: :infinity

    test "RulesTree building" do
      rules_gen = fn rules_num ->
        for i <- 1..rules_num do
          %Rule{
            lql_filters: [
              %FilterRule{
                path: "metadata.rule_id",
                operator: :=,
                value: "rule-#{i}",
                modifiers: %{}
              },
              %FilterRule{
                path: "severity_number",
                operator: :>,
                value: 8,
                modifiers: %{}
              }
            ],
            backend_id: i
          }
        end
      end

      Benchee.run(
        %{
          "build" => fn rules ->
            RulesTree.build(rules)
          end
        },
        inputs: %{
          "100" => rules_gen.(100),
          "1k" => rules_gen.(1000),
          "10k" => rules_gen.(10_000)
        },
        time: 3,
        warmup: 2,
        memory_time: 3,
        reduction_time: 3,
        print: [configuration: false],
        # use extended_statistics to view units of work done
        formatters: [{Benchee.Formatters.Console, extended_statistics: true}]
      )

      # Benchmarking build with input 100 ...
      # Benchmarking build with input 10k ...
      # Benchmarking build with input 1k ...
      # Calculating statistics...
      # Formatting results...

      # ##### With input 100 #####
      # Name            ips        average  deviation         median         99th %
      # build       13.81 K       72.43 μs    ±15.56%       69.67 μs      108.21 μs

      # Extended statistics:

      # Name          minimum        maximum    sample size                     mode
      # build        56.58 μs      746.38 μs        41.31 K                 65.75 μs

      # Memory usage statistics:

      # Name     Memory usage
      # build       160.88 KB

      # **All measurements for memory usage were the same**

      # Reduction count statistics:

      # Name  Reduction count
      # build         18.86 K

      # **All measurements for reduction count were the same**

      # ##### With input 10k #####
      # Name            ips        average  deviation         median         99th %
      # build        116.31        8.60 ms     ±8.72%        8.29 ms       10.94 ms

      # Extended statistics:

      # Name          minimum        maximum    sample size                     mode
      # build         7.50 ms       11.65 ms            349                  8.53 ms

      # Memory usage statistics:

      # Name     Memory usage
      # build        17.29 MB

      # **All measurements for memory usage were the same**

      # Reduction count statistics:

      # Name          average  deviation         median         99th %
      # build          1.68 M     ±0.04%         1.68 M         1.68 M

      # Extended statistics:

      # Name          minimum        maximum    sample size                     mode
      # build          1.68 M         1.68 M            266                   1.68 M

      # ##### With input 1k #####
      # Name            ips        average  deviation         median         99th %
      # build        1.29 K      772.40 μs     ±9.72%      752.29 μs      992.98 μs

      # Extended statistics:

      # Name          minimum        maximum    sample size                     mode
      # build       580.88 μs     1155.17 μs         3.88 K                730.33 μs

      # Memory usage statistics:

      # Name     Memory usage
      # build         1.60 MB

      # **All measurements for memory usage were the same**

      # Reduction count statistics:

      # Name  Reduction count
      # build        172.99 K
    end
  end

  defp filter(path, operator, value, modifiers \\ %{}) do
    %FilterRule{path: path, operator: operator, value: value, modifiers: modifiers}
  end

  defp range_filter(path, l, r) do
    %FilterRule{path: path, operator: :range, values: [l, r], modifiers: %{}}
  end

  defp rule(id, filters), do: %Rule{id: id, lql_filters: filters}

  defp matching(rules, body) do
    tree = RulesTree.build(rules)

    %LogEvent{body: body}
    |> RulesTree.matching_rule_ids(tree)
    |> Enum.sort()
  end
end
