defmodule Logflare.Lql.Parser.ChartParsersTest do
  use ExUnit.Case, async: true

  import NimbleParsec

  alias Logflare.Lql.Parser.ChartParsers

  defparsec(:test_chart_aggregate, ChartParsers.chart_aggregate())
  defparsec(:test_chart_aggregate_group_by, ChartParsers.chart_aggregate_group_by())

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
  end
end
