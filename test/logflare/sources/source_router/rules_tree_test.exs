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
end
