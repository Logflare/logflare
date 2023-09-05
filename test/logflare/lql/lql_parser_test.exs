defmodule Logflare.LqlParserTest do
  @moduledoc false
  use Logflare.DataCase, async: true
  alias Logflare.Lql
  alias Logflare.Lql.{Parser, ChartRule, FilterRule}
  alias Logflare.DateTimeUtils
  alias Logflare.Source.BigQuery.SchemaBuilder
  @default_schema SchemaBuilder.initial_table_schema()

  describe "LQL parsing" do
    test "other top-level fields" do
      schema = build_schema(%{"a" => "t", "b" => %{"c" => %{"d" => "test"}}})
      qs = ~S|a:testing b.c.d:"nested"|

      lql_rules = [
        %FilterRule{operator: :=, path: "a", value: "testing"},
        %FilterRule{
          operator: :=,
          path: "b.c.d",
          value: "nested",
          modifiers: %{quoted_string: true}
        }
      ]

      assert {:ok, lql_rules} == Parser.parse(qs, schema)
      assert Lql.encode!(lql_rules) == qs
    end

    test "regexp" do
      qs = ~S|~new ~"user sign up" ~^error$|

      lql_rules = [
        %FilterRule{operator: :"~", path: "event_message", value: "new"},
        %FilterRule{
          operator: :"~",
          path: "event_message",
          value: "user sign up",
          modifiers: %{quoted_string: true}
        },
        %FilterRule{
          operator: :"~",
          path: "event_message",
          value: "^error$"
        }
      ]

      assert {:ok, lql_rules} == Parser.parse(qs, @default_schema)
      assert Lql.encode!(lql_rules) == qs
    end

    test "regexp double-quote escaping" do
      qs = ~S|~"user \"sign\" up" ~^er\"ror|

      lql_rules = [
        %FilterRule{
          operator: :"~",
          path: "event_message",
          value: ~S(user \"sign\" up),
          modifiers: %{quoted_string: true}
        },
        %FilterRule{
          operator: :"~",
          path: "event_message",
          value: ~S(^er\"ror)
        }
      ]

      assert {:ok, lql_rules} == Parser.parse(qs, @default_schema)
      assert Lql.encode!(lql_rules) == qs
    end

    test "range int/float" do
      schema = build_schema(%{"metadata" => %{"float" => 1.0, "int" => 1}})
      qs = ~S|m.float:30.1..300.1 m.int:50..200|

      lql_rules = [
        %FilterRule{
          operator: :range,
          path: "metadata.float",
          values: [30.1, 300.1]
        },
        %FilterRule{
          operator: :range,
          path: "metadata.int",
          values: [50, 200]
        }
      ]

      assert {:ok, lql_rules} == Parser.parse(qs, schema)
      assert Lql.encode!(lql_rules) == qs
    end

    test "negated filter, full timestamp ranges" do
      schema = build_schema(%{"metadata" => %{"str" => "str", "int" => 1}})

      qs = ~S|
        -m.int:>=100
        -m.str:~a
        -t:2019-01-01T00:13:37..2019-02-01T00:23:34
      |

      lql_rules = [
        %FilterRule{
          modifiers: %{negate: true},
          operator: :>=,
          path: "metadata.int",
          value: 100
        },
        %FilterRule{
          modifiers: %{negate: true},
          operator: :"~",
          path: "metadata.str",
          value: "a"
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{negate: true},
          operator: :range,
          path: "timestamp",
          values: [~N[2019-01-01 00:13:37Z], ~N[2019-02-01 00:23:34Z]]
        }
      ]

      assert {:ok, lql_rules} == Parser.parse(qs, schema)

      assert "-m.int:>=100 -m.str:~a -t:2019-{01..02}-01T00:{13..23}:{37..34}" ==
               Lql.encode!(lql_rules)
    end

    test "timestamp operators, value quoting" do
      schema =
        build_schema(%{
          "metadata" => %{
            "context" => %{
              "file" => "string",
              "address" => "string",
              "line_number" => 1
            }
          }
        })

      qs = ~S|
      unquoted "is quoted"
      m.context.address:~"\\d\\d\\d ST"
      m.context.file:"some module.ex"
      m.context.line_number:100
      t:>2019-01-01
      t:<=2019-04-20
      t:<2020-01-01T03:14:15
      t:>=2019-01-01T03:14:15
      t:<=2020-01-01T00:00:00.345000
      |

      {:ok, result} = Parser.parse(qs, schema)
      rules = result |> Enum.filter(&(&1.path == "timestamp"))
      assert Enum.map(rules, & &1.operator) == [:>, :<=, :<, :>=, :<=]

      assert Enum.map(rules, & &1.value) == [
               ~D[2019-01-01],
               ~D[2019-04-20],
               ~N[2020-01-01 03:14:15Z],
               ~N[2019-01-01 03:14:15Z],
               ~N[2020-01-01 00:00:00.345000Z]
             ]

      rules = result |> Enum.filter(&(&1.path == "event_message"))
      assert Enum.map(rules, & &1.modifiers) == [%{}, %{quoted_string: true}]
      assert Enum.map(rules, & &1.value) == ["unquoted", "is quoted"]
      assert Enum.map(rules, & &1.operator) |> Enum.all?(&(&1 == :string_contains))

      rules = result |> Enum.filter(&(&1.path =~ "metadata.context"))
      assert Enum.map(rules, & &1.operator) == [:"~", :=, :=]

      assert Enum.map(rules, & &1.modifiers) == [
               %{quoted_string: true},
               %{quoted_string: true},
               %{}
             ]

      assert Enum.map(rules, & &1.value) == [~S(\\d\\d\\d ST), "some module.ex", 100]

      assert Lql.encode!(result) ==
               String.split(qs, "\n")
               |> Enum.map(&String.trim/1)
               |> Enum.join(" ")
               |> String.trim()
    end

    test "timestamp shorthands" do
      assert {:ok,
              [
                %FilterRule{
                  operator: :=,
                  path: "timestamp",
                  shorthand: "now",
                  value: now_out
                }
              ]} = Parser.parse("timestamp:now", @default_schema)

      # use diff to reduce test flakiness
      assert DateTime.diff(now_ndt(), now_out, :millisecond) |> abs() <= 150

      for {qs, shorthand, start_value, end_value} <- [
            {"t:today", "today", today_dt(),
             today_dt()
             |> Timex.shift(days: 1)
             |> Timex.shift(seconds: -1)},
            {"t:yesterday", "yesterday", today_dt() |> Timex.shift(days: -1),
             today_dt() |> Timex.shift(seconds: -1)},
            {
              "t:this@minute",
              "this@minute",
              now_udt_zero_sec(),
              DateTimeUtils.truncate(Timex.now(), :second)
            },
            {
              "t:this@hour",
              "this@hour",
              %{now_udt_zero_sec() | minute: 0},
              DateTimeUtils.truncate(Timex.now(), :second)
            },
            {
              "t:this@day",
              "this@day",
              %{now_udt_zero_sec() | minute: 0, hour: 0},
              DateTimeUtils.truncate(Timex.now(), :second)
            },
            {"t:this@week", "this@week",
             Timex.beginning_of_week(%{now_udt_zero_sec() | minute: 0, hour: 0}),
             DateTimeUtils.truncate(Timex.now(), :second)},
            {"t:this@month", "this@month",
             Timex.beginning_of_month(%{now_udt_zero_sec() | minute: 0, hour: 0}),
             DateTimeUtils.truncate(Timex.now(), :second)},
            {"t:this@year", "this@year",
             Timex.beginning_of_year(%{now_udt_zero_sec() | minute: 0, hour: 0}),
             DateTimeUtils.truncate(Timex.now(), :second)},
            {
              "t:last@50s",
              "last@50second",
              Timex.shift(now_ndt(), seconds: -50),
              now_ndt()
            },
            {
              "t:last@43m",
              "last@43minute",
              Timex.shift(now_udt_zero_sec(), minutes: -43),
              now_ndt()
            },
            {"t:last@100h", "last@100hour",
             Timex.shift(%{now_udt_zero_sec() | minute: 0}, hours: -100), now_ndt()},
            {"t:last@7d", "last@7day",
             Timex.shift(%{now_udt_zero_sec() | minute: 0, hour: 0}, days: -7), now_ndt()},
            {"t:last@2w", "last@2week",
             Timex.shift(%{now_udt_zero_sec() | minute: 0, hour: 0}, weeks: -2), now_ndt()},
            {"t:last@1mm", "last@1month",
             Timex.shift(%{now_udt_zero_sec() | minute: 0, hour: 0}, months: -1), now_ndt()},
            {"t:last@1y", "last@1year",
             Timex.shift(%{now_udt_zero_sec() | minute: 0, hour: 0}, years: -1), now_ndt()}
          ] do
        assert {:ok,
                [
                  %Logflare.Lql.FilterRule{
                    operator: :range,
                    path: "timestamp",
                    shorthand: ^shorthand,
                    values: [start_out, end_out]
                  }
                ]} = Parser.parse(qs, @default_schema)

        # use diff to reduce test flakiness
        assert DateTime.diff(start_value, start_out, :millisecond) |> abs() <= 1500
        assert DateTime.diff(end_value, end_out, :millisecond) |> abs() <= 1500
      end
    end

    test "NULL" do
      schema = build_schema(%{"metadata" => %{"nullable" => "val"}})
      qs = "metadata.nullable:NULL"

      lql_rules = [
        %FilterRule{
          operator: :=,
          path: "metadata.nullable",
          value: :NULL
        }
      ]

      assert {:ok, lql_rules} == Parser.parse(qs, schema)
    end

    test "level ranges" do
      schema = build_schema(%{"metadata" => %{"level" => "info"}})

      for {qs, levels} <- [
            {"metadata.level:debug..critical",
             ["debug", "info", "notice", "warning", "error", "critical"]},
            {"metadata.level:notice..warning", ["notice", "warning"]},
            {"metadata.level:debug..error", ["debug", "info", "notice", "warning", "error"]}
          ] do
        lql_rules =
          for level <- levels do
            %FilterRule{
              operator: :=,
              path: "metadata.level",
              value: level
            }
          end

        assert {:ok, lql_rules} == Parser.parse(qs, schema)
      end
    end

    test "list contains operator: string" do
      schema = build_schema(%{"metadata" => %{"arr" => ["a"]}})

      for {qs, modifier} <- [
            {~S(m.arr:@>a), %{}},
            {~S(m.arr:@>"a"), %{quoted_string: true}}
          ] do
        lql_rules = [
          %FilterRule{
            modifiers: modifier,
            operator: :list_includes,
            path: "metadata.arr",
            value: "a"
          }
        ]

        assert {:ok, lql_rules} == Parser.parse(qs, schema)
        assert Lql.encode!(lql_rules) == qs
      end
    end

    test "list contains operator: integer" do
      schema = build_schema(%{"metadata" => %{"arr" => [1]}})

      for {qs, modifier} <- [
            {~S(m.arr:@>1), %{}},
            {~S(m.arr:@>"1"), %{quoted_string: true}}
          ] do
        lql_rules = [
          %FilterRule{
            modifiers: modifier,
            operator: :list_includes,
            path: "metadata.arr",
            value: 1
          }
        ]

        assert {:ok, lql_rules} == Parser.parse(qs, schema)
        assert Lql.encode!(lql_rules) == qs
      end
    end

    test "list contains operator: float" do
      schema = build_schema(%{"metadata" => %{"arr" => [1.0]}})

      for {qs, modifier} <- [
            {~S(m.arr:@>1.0), %{}},
            {~S(m.arr:@>"1.0"), %{quoted_string: true}}
          ] do
        lql_rules = [
          %FilterRule{
            modifiers: modifier,
            operator: :list_includes,
            path: "metadata.arr",
            value: 1.0
          }
        ]

        assert {:ok, lql_rules} == Parser.parse(qs, schema)
        assert Lql.encode!(lql_rules) == qs
      end
    end

    test "lt, range for float values" do
      schema = build_schema(%{"metadata" => %{"metric" => 10.0, "user" => 1.0}})
      qs = "m.metric:<10.0 m.user:0.1..100.111"

      lql_rules = [
        %FilterRule{
          operator: :<,
          path: "metadata.metric",
          value: 10.0
        },
        %FilterRule{
          operator: :range,
          path: "metadata.user",
          values: [0.1, 100.111]
        }
      ]

      assert {:ok, lql_rules} == Parser.parse(qs, schema)
      assert Lql.encode!(lql_rules) == qs
    end

    test "boolean" do
      schema = build_schema(%{"metadata" => %{"isAllowed" => true}})
      qs = "m.isAllowed:true m.isAllowed:false"

      lql_rules = [
        %FilterRule{
          operator: :=,
          path: "metadata.isAllowed",
          value: true
        },
        %FilterRule{
          operator: :=,
          path: "metadata.isAllowed",
          value: false
        }
      ]

      assert {:ok, lql_rules} == Parser.parse(qs, schema)
      assert Lql.encode!(lql_rules) == qs
    end

    test "chart period, chart aggregate" do
      schema = build_schema(%{"metadata" => %{"metric" => 10.0}})
      qs = "c:sum(m.metric) c:group_by(t::minute)"
      assert {:ok, result} = Parser.parse(qs, schema)

      assert result == [
               %ChartRule{
                 path: "metadata.metric",
                 aggregate: :sum,
                 period: :minute,
                 value_type: :float
               }
             ]

      assert Lql.encode!(result) == qs
    end

    test "returns error on malformed timestamp filter" do
      assert {:error, "Error while parsing timestamp" <> _err} =
               Parser.parse("timestamp:>20", @default_schema)
    end

    test "returns human readable error for invalid query" do
      assert {:error, "Error while parsing" <> err} =
               Parser.parse("metadata.user.emailAddress:", @default_schema)

      assert err =~ "emailAddress"
      assert err =~ "metadata filter value"
    end
  end

  describe "LQL parser for timestamp range shorthand" do
    test "year/month/day range" do
      for {qs, start_date, end_date} <- [
            {"t:2020-{05..07}-01", ~D[2020-05-01], ~D[2020-07-01]},
            {"t:2020-05-{01..02}", ~D[2020-05-01], ~D[2020-05-02]},
            {"t:{2010..2020}-05-{01..02}", ~D[2010-05-01], ~D[2020-05-02]}
          ] do
        lql_rules = [
          %FilterRule{
            operator: :range,
            path: "timestamp",
            values: [start_date, end_date]
          }
        ]

        assert {:ok, lql_rules} == Parser.parse(qs, @default_schema)
        assert qs == Lql.encode!(lql_rules)
      end
    end

    test "timestamp filter with leading zero microseconds" do
      qs = "t:>2020-01-01T13:14:15.000500"

      lql_rules = [
        %Logflare.Lql.FilterRule{
          operator: :>,
          path: "timestamp",
          value: ~N[2020-01-01 13:14:15.000500]
        }
      ]

      assert {:ok, lql_rules} == Parser.parse(qs, @default_schema)

      assert qs == Lql.encode!(lql_rules)
    end

    test "timestamp microsecond ranges" do
      for {range, start_micro, end_micro} <- [
            {"0..515", "000000", "515000"},
            {"0101..3555", "010100", "355500"},
            {"1..7", "100000", "700000"},
            {"005001..1", "005001", "100000"}
          ] do
        qs = "t:2020-01-01T13:14:15.{#{range}}"

        lql_rules = [
          %Logflare.Lql.FilterRule{
            operator: :range,
            path: "timestamp",
            values: [
              NaiveDateTime.from_iso8601!("2020-01-01T13:14:15.#{start_micro}"),
              NaiveDateTime.from_iso8601!("2020-01-01T13:14:15.#{end_micro}")
            ]
          }
        ]

        assert {:ok, lql_rules} == Parser.parse(qs, @default_schema)
        assert qs == Lql.encode!(lql_rules)
      end
    end

    test "combined ranges" do
      qs = "t:2020-{01..12}-{01..30}T{00..23}:{15..20}:{35..55}.{000001..585444}"

      lql_rules = [
        %FilterRule{
          operator: :range,
          path: "timestamp",
          values: [
            ~N[2020-01-01 00:15:35.000001],
            ~N[2020-12-30 23:20:55.585444]
          ]
        }
      ]

      assert {:ok, lql_rules} == Parser.parse(qs, @default_schema)
      assert qs == Lql.encode!(lql_rules)
    end
  end

  test "Encoder.to_datetime_with_range/2" do
    for {start_str, end_str, expected} <- [
          # minutes
          {"2020-01-01T03:05:15Z", "2020-01-01T03:59:15Z", "2020-01-01T03:{05..59}:15"},
          # hour
          {"2020-01-01T17:00:15Z", "2020-01-01T23:00:15Z", "2020-01-01T{17..23}:00:15"},
          # day
          {"2020-01-01T17:00:15Z", "2020-01-15T17:00:15Z", "2020-01-{01..15}T17:00:15"},
          # none
          {"2020-12-01T17:00:15Z", "2020-12-01T17:00:15Z", "2020-12-01T17:00:15"}
        ] do
      start_value = NaiveDateTime.from_iso8601!(start_str)
      end_value = NaiveDateTime.from_iso8601!(end_str)
      assert Lql.Encoder.to_datetime_with_range(start_value, end_value) == expected
    end
  end

  def today_dt() do
    Timex.today() |> Timex.to_datetime()
  end

  def now_ndt() do
    %{Timex.now() | microsecond: {0, 0}}
  end

  def now_udt_zero_sec() do
    %{now_ndt() | second: 0}
  end

  def clean_and_trim_lql_string(str) do
    str
    |> String.replace(~r/\s{2,}/, " ")
    |> String.trim()
    |> String.replace_prefix("metadata.", "m.")
    |> String.replace(" metadata.", " m.")
    |> String.replace("(metadata.", "(m.")
  end

  defp build_schema(input) do
    SchemaBuilder.build_table_schema(input, @default_schema)
  end
end
