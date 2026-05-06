defmodule Logflare.Utils.GuardsTest do
  use ExUnit.Case, async: true

  import Logflare.Utils.Guards

  doctest Logflare.Utils.Guards

  describe "is_pos_number/1" do
    test "passes for zero and positive numbers (integers and floats)" do
      assert match?(x when is_pos_number(x), 0)
      assert match?(x when is_pos_number(x), 0.0)
      assert match?(x when is_pos_number(x), 1)
      assert match?(x when is_pos_number(x), 1.5)
      assert match?(x when is_pos_number(x), 1_000_000_000)
    end

    test "fails for negative numbers" do
      refute match?(x when is_pos_number(x), -1)
      refute match?(x when is_pos_number(x), -0.1)
      refute match?(x when is_pos_number(x), -1_000_000)
    end

    test "fails for non-numbers" do
      for value <- ["1", :one, nil, true, [], %{}] do
        refute match?(x when is_pos_number(x), value)
      end
    end
  end

  describe "is_non_negative_integer/1" do
    test "passes for zero and positive integers" do
      assert match?(x when is_non_negative_integer(x), 0)
      assert match?(x when is_non_negative_integer(x), 1)
      assert match?(x when is_non_negative_integer(x), 1_000_000_000)
    end

    test "fails for negative integers" do
      refute match?(x when is_non_negative_integer(x), -1)
      refute match?(x when is_non_negative_integer(x), -1_000_000)
    end

    test "fails for floats, including 0.0" do
      refute match?(x when is_non_negative_integer(x), 0.0)
      refute match?(x when is_non_negative_integer(x), 1.0)
      refute match?(x when is_non_negative_integer(x), 1.5)
    end

    test "fails for non-numbers" do
      for value <- ["1", :one, nil, true, []] do
        refute match?(x when is_non_negative_integer(x), value)
      end
    end
  end

  describe "is_pos_integer/1" do
    test "passes for positive integers" do
      assert match?(x when is_pos_integer(x), 1)
      assert match?(x when is_pos_integer(x), 1_000_000_000)
    end

    test "fails for zero" do
      refute match?(x when is_pos_integer(x), 0)
    end

    test "fails for negative integers" do
      refute match?(x when is_pos_integer(x), -1)
    end

    test "fails for floats" do
      refute match?(x when is_pos_integer(x), 1.0)
      refute match?(x when is_pos_integer(x), 1.5)
    end

    test "fails for non-numbers" do
      for value <- ["1", :one, nil, true, []] do
        refute match?(x when is_pos_integer(x), value)
      end
    end
  end

  describe "is_non_empty_binary/1" do
    test "passes for non-empty binaries" do
      assert match?(x when is_non_empty_binary(x), "a")
      assert match?(x when is_non_empty_binary(x), "hello world")
      assert match?(x when is_non_empty_binary(x), "héllo")
      assert match?(x when is_non_empty_binary(x), " ")
    end

    test "fails for the empty binary" do
      refute match?(x when is_non_empty_binary(x), "")
    end

    test "fails for bitstrings that are not binaries" do
      refute match?(x when is_non_empty_binary(x), <<1::1>>)
    end

    test "fails for non-binaries" do
      refute match?(x when is_non_empty_binary(x), :foo)
      refute match?(x when is_non_empty_binary(x), ~c"foo")
      refute match?(x when is_non_empty_binary(x), nil)
      refute match?(x when is_non_empty_binary(x), 1)
      refute match?(x when is_non_empty_binary(x), [])
    end
  end

  describe "is_atom_value/1" do
    test "passes for regular atoms" do
      assert match?(x when is_atom_value(x), :foo)
      assert match?(x when is_atom_value(x), :ok)
      assert match?(x when is_atom_value(x), Enum)
    end

    test "fails for booleans and nil" do
      refute match?(x when is_atom_value(x), true)
      refute match?(x when is_atom_value(x), false)
      refute match?(x when is_atom_value(x), nil)
    end

    test "fails for non-atoms" do
      refute match?(x when is_atom_value(x), "foo")
      refute match?(x when is_atom_value(x), 1)
      refute match?(x when is_atom_value(x), [])
    end
  end

  describe "is_date/1" do
    test "passes for Date structs" do
      assert match?(x when is_date(x), ~D[2026-04-29])
      assert match?(x when is_date(x), Date.utc_today())
    end

    test "fails for other date/time structs" do
      refute match?(x when is_date(x), ~U[2026-04-29 12:00:00Z])
      refute match?(x when is_date(x), ~N[2026-04-29 12:00:00])
      refute match?(x when is_date(x), ~T[12:00:00])
    end

    test "fails for plain maps and non-structs" do
      for value <- [%{year: 2026, month: 4, day: 29}, %{}, "2026-04-29", nil] do
        refute match?(x when is_date(x), value)
      end
    end
  end

  describe "is_datetime/1" do
    test "passes for DateTime structs" do
      assert match?(x when is_datetime(x), ~U[2026-04-29 12:00:00Z])
      assert match?(x when is_datetime(x), DateTime.utc_now())
    end

    test "fails for NaiveDateTime, Date, and Time" do
      refute match?(x when is_datetime(x), ~N[2026-04-29 12:00:00])
      refute match?(x when is_datetime(x), ~D[2026-04-29])
      refute match?(x when is_datetime(x), ~T[12:00:00])
    end

    test "fails for plain maps and non-structs" do
      for value <- [%{}, "2026-04-29T12:00:00Z", nil] do
        refute match?(x when is_datetime(x), value)
      end
    end
  end

  describe "is_second_precision/1" do
    test "passes for DateTime with microsecond {0, 0}" do
      assert match?(x when is_second_precision(x), ~U[2026-04-29 12:00:00Z])

      truncated = DateTime.utc_now() |> DateTime.truncate(:second)
      assert match?(x when is_second_precision(x), truncated)
    end

    test "fails for DateTime carrying sub-second precision, even if value is zero" do
      refute match?(x when is_second_precision(x), ~U[2026-04-29 12:00:00.000000Z])
      refute match?(x when is_second_precision(x), ~U[2026-04-29 12:00:00.123Z])
      refute match?(x when is_second_precision(x), DateTime.utc_now())
    end

    test "fails for NaiveDateTime even at second precision" do
      refute match?(x when is_second_precision(x), ~N[2026-04-29 12:00:00])
    end

    test "fails for non-datetimes" do
      for value <- [~D[2026-04-29], %{}, nil] do
        refute match?(x when is_second_precision(x), value)
      end
    end
  end

  describe "is_naive_datetime/1" do
    test "passes for NaiveDateTime structs" do
      assert match?(x when is_naive_datetime(x), ~N[2026-04-29 12:00:00])
      assert match?(x when is_naive_datetime(x), NaiveDateTime.utc_now())
    end

    test "fails for DateTime, Date, and Time" do
      refute match?(x when is_naive_datetime(x), ~U[2026-04-29 12:00:00Z])
      refute match?(x when is_naive_datetime(x), ~D[2026-04-29])
      refute match?(x when is_naive_datetime(x), ~T[12:00:00])
    end

    test "fails for non-structs" do
      for value <- [%{}, "2026-04-29 12:00:00", nil] do
        refute match?(x when is_naive_datetime(x), value)
      end
    end
  end

  describe "is_percentile_aggregate/1" do
    test "passes for the supported percentile atoms" do
      assert match?(x when is_percentile_aggregate(x), :p50)
      assert match?(x when is_percentile_aggregate(x), :p95)
      assert match?(x when is_percentile_aggregate(x), :p99)
    end

    test "fails for other percentile-like atoms" do
      refute match?(x when is_percentile_aggregate(x), :p25)
      refute match?(x when is_percentile_aggregate(x), :p75)
      refute match?(x when is_percentile_aggregate(x), :p90)
      refute match?(x when is_percentile_aggregate(x), :avg)
    end

    test "fails for stringified percentiles and other types" do
      refute match?(x when is_percentile_aggregate(x), "p50")
      refute match?(x when is_percentile_aggregate(x), 50)
      refute match?(x when is_percentile_aggregate(x), nil)
    end
  end

  describe "is_list_or_map/1" do
    test "passes for lists" do
      assert match?(x when is_list_or_map(x), [])
      assert match?(x when is_list_or_map(x), [1, 2, 3])
      assert match?(x when is_list_or_map(x), foo: 1, bar: 2)
    end

    test "passes for maps, including structs" do
      assert match?(x when is_list_or_map(x), %{})
      assert match?(x when is_list_or_map(x), %{a: 1})
      assert match?(x when is_list_or_map(x), ~D[2026-04-29])
    end

    test "fails for tuples, strings, atoms, numbers, and nil" do
      refute match?(x when is_list_or_map(x), {1, 2})
      refute match?(x when is_list_or_map(x), "list")
      refute match?(x when is_list_or_map(x), :list)
      refute match?(x when is_list_or_map(x), 1)
      refute match?(x when is_list_or_map(x), nil)
    end
  end

  describe "is_event_type/1" do
    test "passes for the supported event-type atoms" do
      assert match?(x when is_event_type(x), :log)
      assert match?(x when is_event_type(x), :metric)
      assert match?(x when is_event_type(x), :trace)
    end

    test "fails for other atoms" do
      refute match?(x when is_event_type(x), :span)
      refute match?(x when is_event_type(x), :error)
      refute match?(x when is_event_type(x), :logs)
    end

    test "fails for booleans and nil (composed via is_atom_value)" do
      refute match?(x when is_event_type(x), true)
      refute match?(x when is_event_type(x), false)
      refute match?(x when is_event_type(x), nil)
    end

    test "fails for stringified event types and other non-atoms" do
      refute match?(x when is_event_type(x), "log")
      refute match?(x when is_event_type(x), 1)
      refute match?(x when is_event_type(x), [])
    end
  end
end
