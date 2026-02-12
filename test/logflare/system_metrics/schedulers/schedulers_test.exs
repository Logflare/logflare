defmodule Logflare.SystemMetrics.SchedulersTest do
  use ExUnit.Case, async: true
  alias Logflare.SystemMetrics.Schedulers

  describe "scheduler_utilization/2" do
    test "formats scheduler utilization data correctly" do
      # Create two samples
      sample_a = :scheduler.sample()
      # Sleep briefly to create some utilization difference
      Process.sleep(10)
      sample_b = :scheduler.sample()

      result = Schedulers.scheduler_utilization(sample_a, sample_b)

      # Verify result structure
      assert is_list(result)
      assert length(result) > 0

      # Check each metric has expected fields
      Enum.each(result, fn metric ->
        assert is_map(metric)
        assert Map.has_key?(metric, :name)
        assert Map.has_key?(metric, :type)
        assert Map.has_key?(metric, :utilization)
        assert Map.has_key?(metric, :utilization_percentage)

        # Verify types
        assert is_binary(metric.name)
        assert is_binary(metric.type)
        assert is_integer(metric.utilization)
        assert is_float(metric.utilization_percentage)

        # Verify type is one of expected values
        assert metric.type in ["normal", "dirty", "total"]

        # Verify utilization is within reasonable bounds (0-100 * 100)
        assert metric.utilization >= 0
        assert metric.utilization_percentage >= 0.0
        assert metric.utilization_percentage <= 100.0
      end)
    end

    test "includes total scheduler metrics" do
      sample_a = :scheduler.sample()
      Process.sleep(10)
      sample_b = :scheduler.sample()

      result = Schedulers.scheduler_utilization(sample_a, sample_b)

      # Should have at least one "total" entry
      total_metrics = Enum.filter(result, &(&1.type == "total"))
      assert length(total_metrics) > 0

      total = List.first(total_metrics)
      assert total.name == "total"
    end

    test "includes normal and dirty scheduler types" do
      sample_a = :scheduler.sample()
      Process.sleep(10)
      sample_b = :scheduler.sample()

      result = Schedulers.scheduler_utilization(sample_a, sample_b)

      types = Enum.map(result, & &1.type) |> Enum.uniq()

      # Should have normal schedulers
      assert "normal" in types

      # May or may not have dirty schedulers depending on system config
      # but if present, should be formatted correctly
      dirty_schedulers = Enum.filter(result, &(&1.type == "dirty"))

      Enum.each(dirty_schedulers, fn scheduler ->
        assert is_binary(scheduler.name)
        assert scheduler.utilization >= 0
      end)
    end

    test "scheduler names are strings" do
      sample_a = :scheduler.sample()
      Process.sleep(10)
      sample_b = :scheduler.sample()

      result = Schedulers.scheduler_utilization(sample_a, sample_b)

      # All names should be strings (converted from integers)
      Enum.each(result, fn metric ->
        assert is_binary(metric.name)

        # Non-total entries should be numeric strings
        if metric.type != "total" do
          assert String.match?(metric.name, ~r/^\d+$/)
        end
      end)
    end
  end
end
