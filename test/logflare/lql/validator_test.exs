defmodule Logflare.Lql.ValidatorTest do
  use Logflare.DataCase, async: true

  alias Logflare.Lql.ChartRule
  alias Logflare.Lql.FilterRule
  alias Logflare.Lql.Validator

  describe "validate/2" do
    test "returns nil for valid LQL rules" do
      lql_rules = %{
        lql_ts_filters: [],
        chart_period: :minute,
        chart_rules: []
      }

      result = Validator.validate(lql_rules, tailing?: false)
      assert is_nil(result)
    end

    test "returns error when tailing? opt is true and timestamp filters exist" do
      lql_rules = %{
        lql_ts_filters: [
          %FilterRule{
            path: "timestamp",
            operator: :>,
            value: ~N[2023-01-01 00:00:00]
          }
        ],
        chart_period: :minute,
        chart_rules: []
      }

      result = Validator.validate(lql_rules, tailing?: true)
      assert result == "Timestamp filters can't be used if live tail search is active"
    end

    test "raises an error when tailing? option is provided as a non-boolean" do
      lql_rules = %{
        lql_ts_filters: [],
        chart_period: :minute,
        chart_rules: []
      }

      assert_raise ArgumentError, "tailing? must be a boolean value", fn ->
        Validator.validate(lql_rules, tailing?: "invalid")
      end
    end

    test "passes when tailing? is true but no timestamp filters exist" do
      lql_rules = %{
        lql_ts_filters: [],
        chart_period: :minute,
        chart_rules: []
      }

      result = Validator.validate(lql_rules, tailing?: true)
      assert is_nil(result)
    end

    test "passes when tailing? is false even with timestamp filters" do
      lql_rules = %{
        lql_ts_filters: [
          %FilterRule{
            path: "timestamp",
            operator: :>,
            value: ~N[2023-01-01 00:00:00]
          }
        ],
        chart_period: :minute,
        chart_rules: []
      }

      result = Validator.validate(lql_rules, tailing?: false)

      assert result =~
               "The interval length between min and max timestamp is larger than 250 periods"
    end

    test "returns error when multiple chart rules exist" do
      lql_rules = %{
        lql_ts_filters: [],
        chart_period: :minute,
        chart_rules: [
          %ChartRule{
            path: "metadata.latency",
            aggregate: :avg,
            period: :minute,
            value_type: :float
          },
          %ChartRule{
            path: "metadata.count",
            aggregate: :sum,
            period: :minute,
            value_type: :integer
          }
        ]
      }

      result = Validator.validate(lql_rules, tailing?: false)
      assert result == "Only one chart rule can be used in a LQL query"
    end

    test "returns error when exactly one chart rule uses non-numeric field type" do
      lql_rules = %{
        lql_ts_filters: [],
        chart_period: :minute,
        chart_rules: [
          %ChartRule{
            path: "metadata.user_name",
            aggregate: :avg,
            period: :minute,
            value_type: :string
          }
        ]
      }

      result = Validator.validate(lql_rules, tailing?: false)

      assert result =~
               "Can't aggregate on a non-numeric field type 'string' for path metadata.user_name"
    end

    test "passes when exactly one chart rule uses numeric field type" do
      lql_rules = %{
        lql_ts_filters: [],
        chart_period: :minute,
        chart_rules: [
          %ChartRule{
            path: "metadata.count",
            aggregate: :sum,
            period: :minute,
            value_type: :integer
          }
        ]
      }

      result = Validator.validate(lql_rules, tailing?: false)
      assert is_nil(result)
    end

    test "allows chart rule on timestamp field regardless of type" do
      lql_rules = %{
        lql_ts_filters: [],
        chart_period: :minute,
        chart_rules: [
          %ChartRule{
            path: "timestamp",
            aggregate: :count,
            period: :minute,
            value_type: :string
          }
        ]
      }

      result = Validator.validate(lql_rules, tailing?: false)
      assert is_nil(result)
    end

    test "allows chart rule on integer field" do
      lql_rules = %{
        lql_ts_filters: [],
        chart_period: :minute,
        chart_rules: [
          %ChartRule{
            path: "metadata.count",
            aggregate: :sum,
            period: :minute,
            value_type: :integer
          }
        ]
      }

      result = Validator.validate(lql_rules, tailing?: false)
      assert is_nil(result)
    end

    test "allows chart rule on float field" do
      lql_rules = %{
        lql_ts_filters: [],
        chart_period: :minute,
        chart_rules: [
          %ChartRule{
            path: "metadata.latency",
            aggregate: :avg,
            period: :minute,
            value_type: :float
          }
        ]
      }

      result = Validator.validate(lql_rules, tailing?: false)
      assert is_nil(result)
    end

    test "returns error when chart period is longer than timestamp filter interval" do
      # Create a timestamp filter with a very short interval
      start_time = ~N[2023-01-01 00:00:00]
      # 1 minute interval
      end_time = ~N[2023-01-01 00:01:00]

      lql_rules = %{
        lql_ts_filters: [
          %FilterRule{
            path: "timestamp",
            operator: :range,
            values: [start_time, end_time]
          }
        ],
        # Chart period is longer than the 1-minute interval
        chart_period: :hour,
        chart_rules: []
      }

      result = Validator.validate(lql_rules, tailing?: false)

      assert result ==
               "Selected chart period hour is longer than the timestamp filter interval. Please select a shorter chart period."
    end

    test "returns error when too many chart ticks would be generated" do
      # Create a very long timestamp filter interval with short chart period
      start_time = ~N[2023-01-01 00:00:00]
      # 14 days
      end_time = ~N[2023-01-15 00:00:00]

      lql_rules = %{
        lql_ts_filters: [
          %FilterRule{
            path: "timestamp",
            operator: :range,
            values: [start_time, end_time]
          }
        ],
        # Would generate too many ticks
        chart_period: :minute,
        chart_rules: []
      }

      result = Validator.validate(lql_rules, tailing?: false)

      assert result =~
               "The interval length between min and max timestamp is larger than 250 periods"
    end

    test "works with single timestamp filter" do
      lql_rules = %{
        lql_ts_filters: [
          %FilterRule{
            path: "timestamp",
            operator: :>,
            value: ~N[2023-01-01 00:00:00]
          }
        ],
        chart_period: :minute,
        chart_rules: []
      }

      result = Validator.validate(lql_rules, tailing?: false)

      assert result =~
               "The interval length between min and max timestamp is larger than 250 periods"
    end

    test "works with multiple timestamp filters" do
      lql_rules = %{
        lql_ts_filters: [
          %FilterRule{
            path: "timestamp",
            operator: :>,
            value: ~N[2023-01-01 00:00:00]
          },
          %FilterRule{
            path: "timestamp",
            operator: :<,
            value: ~N[2023-01-02 00:00:00]
          }
        ],
        chart_period: :hour,
        chart_rules: []
      }

      result = Validator.validate(lql_rules, tailing?: false)
      assert is_nil(result)
    end

    test "validates with reasonable timestamp range and chart period" do
      start_time = ~N[2023-01-01 00:00:00]
      # 2 hours (120 minutes, under 250 limit)
      end_time = ~N[2023-01-01 02:00:00]

      lql_rules = %{
        lql_ts_filters: [
          %FilterRule{
            path: "timestamp",
            operator: :range,
            values: [start_time, end_time]
          }
        ],
        # 120 ticks, should be fine
        chart_period: :minute,
        chart_rules: [
          %ChartRule{
            path: "metadata.latency",
            aggregate: :avg,
            period: :minute,
            value_type: :float
          }
        ]
      }

      result = Validator.validate(lql_rules, tailing?: false)
      assert is_nil(result)
    end

    test "validates exactly one chart rule condition for non-numeric type" do
      start_time = ~N[2023-01-01 00:00:00]
      end_time = ~N[2023-01-01 02:00:00]

      lql_rules = %{
        lql_ts_filters: [
          %FilterRule{
            path: "timestamp",
            operator: :range,
            values: [start_time, end_time]
          }
        ],
        chart_period: :minute,
        chart_rules: [
          %ChartRule{
            path: "metadata.status",
            aggregate: :count,
            period: :minute,
            value_type: :boolean
          }
        ]
      }

      result = Validator.validate(lql_rules, tailing?: false)

      assert result =~
               "Can't aggregate on a non-numeric field type 'boolean' for path metadata.status"
    end
  end
end
