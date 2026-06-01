defmodule Logflare.LogEvent.DayBucketTest do
  use ExUnit.Case, async: false

  doctest Logflare.LogEvent.DayBucket

  alias Logflare.LogEvent.DayBucket

  @microseconds_per_day 86_400 * 1_000_000
  @pt_key {DayBucket, :current}

  describe "from_microseconds/1" do
    test "returns 0 for the epoch" do
      assert DayBucket.from_microseconds(0) == 0
    end

    test "buckets a whole-day timestamp" do
      assert DayBucket.from_microseconds(@microseconds_per_day) == 1
      assert DayBucket.from_microseconds(@microseconds_per_day * 100) == 100
    end

    test "truncates within a day toward zero" do
      assert DayBucket.from_microseconds(@microseconds_per_day - 1) == 0
      assert DayBucket.from_microseconds(@microseconds_per_day + 1) == 1
      assert DayBucket.from_microseconds(2 * @microseconds_per_day - 1) == 1
    end

    test "returns negative buckets for pre-epoch timestamps" do
      assert DayBucket.from_microseconds(-@microseconds_per_day) == -1
      assert DayBucket.from_microseconds(-2 * @microseconds_per_day) == -2
      assert DayBucket.from_microseconds(-1) == 0
    end

    test "raises FunctionClauseError for non-integer input" do
      assert_raise FunctionClauseError, fn -> DayBucket.from_microseconds(1.5) end
      assert_raise FunctionClauseError, fn -> DayBucket.from_microseconds("123") end
      assert_raise FunctionClauseError, fn -> DayBucket.from_microseconds(nil) end
    end
  end

  describe "classify_freshness/1" do
    setup do
      [current: DayBucket.current()]
    end

    test "current day is :fresh", %{current: current} do
      assert DayBucket.classify_freshness(current) == :fresh
    end

    test "one day in the past is :fresh", %{current: current} do
      assert DayBucket.classify_freshness(current - 1) == :fresh
    end

    test "one day in the future is :fresh", %{current: current} do
      assert DayBucket.classify_freshness(current + 1) == :fresh
    end

    test "two days in the past is :stale", %{current: current} do
      assert DayBucket.classify_freshness(current - 2) == :stale
    end

    test "two days in the future is :stale", %{current: current} do
      assert DayBucket.classify_freshness(current + 2) == :stale
    end

    test "pre-epoch (negative) bucket is :stale" do
      assert DayBucket.classify_freshness(-1) == :stale
      assert DayBucket.classify_freshness(-1000) == :stale
    end

    test "far-future bucket is :stale", %{current: current} do
      assert DayBucket.classify_freshness(current + 365) == :stale
    end
  end

  describe "current/0" do
    test "returns today's bucket from the persistent_term cache" do
      expected = div(System.system_time(:microsecond), @microseconds_per_day)
      assert DayBucket.current() == expected
    end

    test "falls back to on-demand compute when the cache is missing" do
      cached = :persistent_term.get(@pt_key)
      :persistent_term.erase(@pt_key)

      try do
        expected = div(System.system_time(:microsecond), @microseconds_per_day)
        assert DayBucket.current() == expected
      after
        :persistent_term.put(@pt_key, cached)
      end
    end
  end
end
