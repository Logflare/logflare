defmodule Logflare.Lql.EncoderTest do
  use ExUnit.Case, async: true

  alias Logflare.Lql.Encoder
  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Lql.Rules.SelectRule

  describe "to_querystring/1" do
    test "encodes single filter rule" do
      lql_rules = [
        %FilterRule{operator: :=, path: "event_message", value: "test"}
      ]

      result = Encoder.to_querystring(lql_rules)
      assert result == "test"
    end

    test "encodes multiple filter rules for same path" do
      lql_rules = [
        %FilterRule{operator: :=, path: "m.user", value: "alice"},
        %FilterRule{operator: :=, path: "m.user", value: "bob"}
      ]

      result = Encoder.to_querystring(lql_rules)
      assert result == "m.user:alice m.user:bob"
    end

    test "encodes multiple level filters as range" do
      lql_rules = [
        %FilterRule{operator: :=, path: "m.level", value: "error"},
        %FilterRule{operator: :=, path: "m.level", value: "debug"},
        %FilterRule{operator: :=, path: "m.level", value: "warning"}
      ]

      result = Encoder.to_querystring(lql_rules)
      assert result == "m.level:debug..error"
    end

    test "encodes chart rules" do
      lql_rules = [
        %ChartRule{
          path: "metadata.metric",
          aggregate: :sum,
          period: :minute,
          value_type: nil
        }
      ]

      result = Encoder.to_querystring(lql_rules)
      assert result == "c:sum(m.metric) c:group_by(t::minute)"
    end

    test "encodes mixed filter and chart rules" do
      lql_rules = [
        %FilterRule{operator: :=, path: "m.status", value: "success"},
        %ChartRule{
          path: "timestamp",
          aggregate: :count,
          period: :hour,
          value_type: nil
        }
      ]

      result = Encoder.to_querystring(lql_rules)
      assert result == "m.status:success c:count(*) c:group_by(t::hour)"
    end

    test "encodes negated filter rules" do
      lql_rules = [
        %FilterRule{
          operator: :=,
          path: "m.status",
          value: "error",
          modifiers: %{negate: true}
        }
      ]

      result = Encoder.to_querystring(lql_rules)
      assert result == "-m.status:error"
    end

    test "encodes timestamp range" do
      lql_rules = [
        %FilterRule{
          operator: :range,
          path: "timestamp",
          values: [~N[2020-01-01 10:30:00], ~N[2020-01-01 11:45:00]]
        }
      ]

      result = Encoder.to_querystring(lql_rules)
      assert result == "t:2020-01-01T{10..11}:{30..45}:00"
    end

    test "encodes quoted string filter" do
      lql_rules = [
        %FilterRule{
          operator: :=,
          path: "event_message",
          value: "hello world",
          modifiers: %{quoted_string: true}
        }
      ]

      result = Encoder.to_querystring(lql_rules)
      assert result == ~s|"hello world"|
    end

    test "encodes empty list" do
      result = Encoder.to_querystring([])
      assert result == ""
    end
  end

  describe "to_datetime_with_range/2" do
    test "formats date ranges" do
      start_date = ~D[2020-01-15]
      end_date = ~D[2020-01-20]

      result = Encoder.to_datetime_with_range(start_date, end_date)
      assert result == "2020-01-{15..20}"
    end

    test "formats same dates" do
      date = ~D[2020-01-15]

      result = Encoder.to_datetime_with_range(date, date)
      assert result == "2020-01-15"
    end

    test "formats datetime ranges" do
      start_dt = ~N[2020-01-01 03:05:15]
      end_dt = ~N[2020-01-01 03:59:15]

      result = Encoder.to_datetime_with_range(start_dt, end_dt)
      assert result == "2020-01-01T03:{05..59}:15"
    end

    test "formats datetime with same values" do
      datetime = ~N[2020-01-01 12:30:45]

      result = Encoder.to_datetime_with_range(datetime, datetime)
      assert result == "2020-01-01T12:30:45"
    end

    test "formats datetime with microseconds" do
      start_dt = ~N[2020-01-01 12:30:45.123000]
      end_dt = ~N[2020-01-01 12:30:45.456000]

      result = Encoder.to_datetime_with_range(start_dt, end_dt)
      assert result == "2020-01-01T12:30:45.{123..456}"
    end

    test "formats datetime with same microseconds" do
      start_dt = ~N[2020-01-01 12:30:45.123000]
      end_dt = ~N[2020-01-01 12:30:45.123000]

      result = Encoder.to_datetime_with_range(start_dt, end_dt)
      assert result == "2020-01-01T12:30:45.123"
    end

    test "formats datetime with zero microseconds" do
      start_dt = ~N[2020-01-01 12:30:45.000000]
      end_dt = ~N[2020-01-01 12:30:45.000000]

      result = Encoder.to_datetime_with_range(start_dt, end_dt)
      assert result == "2020-01-01T12:30:45"
    end

    test "formats complex datetime range" do
      start_dt = ~N[2020-01-01 10:15:30.100000]
      end_dt = ~N[2020-12-31 23:45:59.900000]

      result = Encoder.to_datetime_with_range(start_dt, end_dt)
      assert result == "2020-{01..12}-{01..31}T{10..23}:{15..45}:{30..59}.{1..9}"
    end

    test "formats datetime ranges from ISO8601 strings" do
      for {start_str, end_str, expected} <- [
            {"2020-01-01T03:05:15Z", "2020-01-01T03:59:15Z", "2020-01-01T03:{05..59}:15"},
            {"2020-01-01T17:00:15Z", "2020-01-01T23:00:15Z", "2020-01-01T{17..23}:00:15"},
            {"2020-01-01T17:00:15Z", "2020-01-15T17:00:15Z", "2020-01-{01..15}T17:00:15"},
            {"2020-12-01T17:00:15Z", "2020-12-01T17:00:15Z", "2020-12-01T17:00:15"}
          ] do
        start_value = NaiveDateTime.from_iso8601!(start_str)
        end_value = NaiveDateTime.from_iso8601!(end_str)
        assert Encoder.to_datetime_with_range(start_value, end_value) == expected
      end
    end

    test "encodes select rules" do
      lql_rules = [
        %SelectRule{path: "event_message", wildcard: false}
      ]

      result = Encoder.to_querystring(lql_rules)
      assert result == "s:event_message"
    end

    test "encodes wildcard select rule" do
      lql_rules = [
        %SelectRule{path: "*", wildcard: true}
      ]

      result = Encoder.to_querystring(lql_rules)
      assert result == "s:*"
    end

    test "encodes nested field select rules" do
      lql_rules = [
        %SelectRule{path: "metadata.user.id", wildcard: false}
      ]

      result = Encoder.to_querystring(lql_rules)
      assert result == "s:m.user.id"
    end

    test "encodes multiple select rules" do
      lql_rules = [
        %SelectRule{path: "event_message", wildcard: false},
        %SelectRule{path: "timestamp", wildcard: false},
        %SelectRule{path: "metadata.user.id", wildcard: false}
      ]

      result = Encoder.to_querystring(lql_rules)
      assert result == "s:event_message s:timestamp s:m.user.id"
    end

    test "encodes mixed rule types in correct order" do
      lql_rules = [
        %FilterRule{operator: :=, path: "event_message", value: "error"},
        %SelectRule{path: "metadata.level", wildcard: false},
        %ChartRule{path: "timestamp", aggregate: :count, period: :minute}
      ]

      result = Encoder.to_querystring(lql_rules)
      assert result == "s:m.level error c:count(*) c:group_by(t::minute)"
    end
  end
end
