defmodule Logflare.Lql.Parser.ChartParsersTest do
  use ExUnit.Case, async: true

  import NimbleParsec

  alias Logflare.Lql.Parser.ChartParsers

  defparsec(:test_chart_aggregate, ChartParsers.chart_aggregate())
  defparsec(:test_chart_aggregate_group_by, ChartParsers.chart_aggregate_group_by())
  defparsec(:test_chart_clause, ChartParsers.chart_clause())

  describe "chart parsing combinators" do
    test "chart aggregate parser" do
      assert {:ok, [aggregate: :count, path: "timestamp"], "", _, _, _} =
               test_chart_aggregate("count(*)")

      assert {:ok, [aggregate: :avg, path: "metadata.duration"], "", _, _, _} =
               test_chart_aggregate("avg(metadata.duration)")

      assert {:ok, [aggregate: :sum, path: "metadata.size"], "", _, _, _} =
               test_chart_aggregate("sum(metadata.size)")

      assert {:ok, [aggregate: :max, path: "metadata.value"], "", _, _, _} =
               test_chart_aggregate("max(metadata.value)")

      assert {:ok, [aggregate: :p50, path: "metadata.latency"], "", _, _, _} =
               test_chart_aggregate("p50(metadata.latency)")

      assert {:ok, [aggregate: :p95, path: "metadata.latency"], "", _, _, _} =
               test_chart_aggregate("p95(metadata.latency)")

      assert {:ok, [aggregate: :p99, path: "metadata.latency"], "", _, _, _} =
               test_chart_aggregate("p99(metadata.latency)")
    end

    test "chart aggregate group by parser" do
      assert {:ok, [period: :second], "", _, _, _} =
               test_chart_aggregate_group_by("group_by(timestamp::second)")

      assert {:ok, [period: :second], "", _, _, _} =
               test_chart_aggregate_group_by("group_by(t::s)")

      assert {:ok, [period: :minute], "", _, _, _} =
               test_chart_aggregate_group_by("group_by(timestamp::minute)")

      assert {:ok, [period: :minute], "", _, _, _} =
               test_chart_aggregate_group_by("group_by(t::m)")

      assert {:ok, [period: :hour], "", _, _, _} =
               test_chart_aggregate_group_by("group_by(timestamp::hour)")

      assert {:ok, [period: :hour], "", _, _, _} =
               test_chart_aggregate_group_by("group_by(t::h)")

      assert {:ok, [period: :day], "", _, _, _} =
               test_chart_aggregate_group_by("group_by(timestamp::day)")

      assert {:ok, [period: :day], "", _, _, _} =
               test_chart_aggregate_group_by("group_by(t::d)")
    end

    test "chart clause parser with tag" do
      # Test chart aggregate clause
      result = test_chart_clause("c:count(*)")
      assert match?({:ok, [chart: [aggregate: :count, path: "timestamp"]], "", _, _, _}, result)

      result = test_chart_clause("chart:sum(metadata.metric)")

      assert match?(
               {:ok, [chart: [aggregate: :sum, path: "metadata.metric"]], "", _, _, _},
               result
             )

      # Test chart group_by clause
      result = test_chart_clause("c:group_by(t::minute)")
      assert match?({:ok, [chart: [period: :minute]], "", _, _, _}, result)

      result = test_chart_clause("chart:group_by(timestamp::hour)")
      assert match?({:ok, [chart: [period: :hour]], "", _, _, _}, result)
    end
  end
end
