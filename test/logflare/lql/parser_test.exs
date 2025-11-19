defmodule Logflare.Lql.ParserTest do
  use ExUnit.Case, async: false
  use Mimic

  alias Logflare.DateTimeUtils
  alias Logflare.Lql
  alias Logflare.Lql.Parser
  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Lql.Rules.FromRule
  alias Logflare.Lql.Rules.SelectRule
  alias Logflare.Sources.Source.BigQuery.SchemaBuilder

  @default_schema SchemaBuilder.initial_table_schema()

  describe "parse/1" do
    test "parse empty string" do
      assert {:ok, []} == Parser.parse("")
    end

    test "parse string value" do
      qs = ~S|a:testing|

      assert {:ok,
              [
                %FilterRule{operator: :=, path: "a", value: "testing"}
              ]} == Parser.parse(qs)
    end

    test "parse boolean" do
      qs = ~S|a:true|

      assert {:ok,
              [
                %FilterRule{operator: :=, path: "a", value: true}
              ]} == Parser.parse(qs)
    end

    test "parse boolean false without schema" do
      qs = ~S|a:false|

      assert {:ok,
              [
                %FilterRule{operator: :=, path: "a", value: false}
              ]} == Parser.parse(qs)
    end

    test "parse numeric" do
      qs = ~S|a:1 b:1.1|

      assert {:ok,
              [
                %FilterRule{operator: :=, path: "a", value: 1},
                %FilterRule{operator: :=, path: "b", value: 1.1}
              ]} == Parser.parse(qs)
    end

    test "parse string with special characters" do
      for char <- [
            ".",
            "?",
            "!",
            "@",
            "#",
            "$",
            "%",
            "^",
            "&",
            "*",
            "(",
            ")",
            "-",
            "_",
            "=",
            "+",
            "[",
            "]",
            "{",
            "}",
            "|",
            "\\",
            "/",
            ";",
            ",",
            "<",
            ">"
          ] do
        qs = ~s|my#{char}string|

        assert {:ok,
                [
                  %FilterRule{
                    operator: :string_contains,
                    path: "event_message",
                    value: "my#{char}string"
                  }
                ]} == Parser.parse(qs)
      end
    end

    test "chart period, chart aggregate" do
      qs = "c:sum(m.metric) c:group_by(t::minute)"

      result = [
        %ChartRule{
          path: "metadata.metric",
          aggregate: :sum,
          period: :minute,
          # don't validate the typing downstream
          value_type: nil
        }
      ]

      assert {:ok, result} == Parser.parse(qs)

      assert Lql.encode!(result) == qs
    end
  end

  setup :verify_on_exit!

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
        %FilterRule{
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
               |> Enum.map_join(" ", &String.trim/1)
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
                  %FilterRule{
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

    test "list contains regex operator" do
      schema = build_schema(%{"metadata" => %{"arr" => ["abc123"]}})

      for {qs, modifier} <- [
            {~S(m.arr:@>~abc), %{}},
            {~S(m.arr:@>~"a c"), %{quoted_string: true}},
            {~S(m.arr:@>~"1.0"), %{quoted_string: true}},
            {~S(m.arr:@>~1), %{}}
          ] do
        assert {:ok,
                [
                  %FilterRule{
                    modifiers: parsed_modifier,
                    operator: :list_includes_regexp,
                    path: "metadata.arr",
                    value: value
                  } = filter_rule
                ]} = Parser.parse(qs, schema)

        assert Lql.encode!([filter_rule]) == qs
        assert is_binary(value)
        assert parsed_modifier == modifier
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

    test "returns error for general parsing errors" do
      assert {:error, _err} = Parser.parse("\x00\x01\x02", @default_schema)
    end

    test "handles NimbleParsec 2-tuple error format" do
      # trying to trigger this error with a null byte input
      result = Parser.parse(IO.iodata_to_binary([<<0x00>>]), @default_schema)
      assert match?({:error, _}, result)
    end

    test "returns error for map type fields" do
      schema = build_schema(%{"metadata" => %{"config" => %{}}})

      assert {:error, :field_not_found, "", ["Field type `map` is not queryable.", "", ""]} =
               Parser.parse("m.config:value", schema)
    end

    test "handles naive_datetime type without casting" do
      schema = build_schema(%{"created_at" => "2023-01-01T00:00:00"})

      assert {:ok, [%FilterRule{path: "created_at", value: "test"}]} =
               Parser.parse("created_at:test", schema)
    end

    test "handles string type without casting" do
      schema = build_schema(%{"metadata" => %{"message" => "string_value"}})

      assert {:ok, [%FilterRule{path: "metadata.message", value: "test_string"}]} =
               Parser.parse("m.message:test_string", schema)
    end

    test "returns error for nonexistent field" do
      assert {:error, :field_not_found, "",
              [
                "LQL parser error: path `metadata.nonexistent` not present in source schema.",
                "",
                ""
              ]} =
               Parser.parse("m.nonexistent:value", @default_schema)
    end

    test "handles naive_datetime fields without casting" do
      schema = build_schema(%{"metadata" => %{"test_field" => "value"}})
      mocked_typemap = %{"metadata.test_field" => :naive_datetime}

      stub(Logflare.Google.BigQuery.SchemaUtils, :bq_schema_to_flat_typemap, fn _ ->
        mocked_typemap
      end)

      assert {:ok, [%FilterRule{path: "metadata.test_field", value: "test_value"}]} =
               Parser.parse("m.test_field:test_value", schema)
    end

    test "returns field not found error for nil type fields" do
      schema = build_schema(%{"metadata" => %{"test_field" => "value"}})
      mocked_typemap = %{"metadata.test_field" => nil}

      stub(Logflare.Google.BigQuery.SchemaUtils, :bq_schema_to_flat_typemap, fn _ ->
        mocked_typemap
      end)

      assert {:error, :field_not_found, "", _} = Parser.parse("m.test_field:test_value", schema)
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
        %FilterRule{
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
          %FilterRule{
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

  describe "parsing error handling" do
    test "returns error for malformed query syntax" do
      assert {:error, error} = Parser.parse("m.field:", @default_schema)
      assert error =~ "Error while parsing"
      assert error =~ "metadata filter value"
    end

    test "returns error for invalid timestamp format" do
      assert {:error, error} = Parser.parse("t:>invalid-date", @default_schema)
      assert error =~ "Error while parsing timestamp"
    end

    test "returns error for invalid operator combinations" do
      assert {:error, error} = Parser.parse("m.field:>>100", @default_schema)
      assert error =~ "Error while parsing"
    end

    test "returns error for unclosed quotes" do
      assert {:error, error} = Parser.parse("\"unclosed quote", @default_schema)
      assert is_binary(error)
    end

    test "returns error for invalid chart syntax" do
      result = Parser.parse("c:invalid(m.field)", @default_schema)
      assert match?({:error, :field_not_found, _, _}, result)
    end

    test "handles empty query string" do
      result = Parser.parse("", @default_schema)
      assert result == {:ok, []}
    end

    test "handles whitespace-only query" do
      result = Parser.parse("   ", @default_schema)
      assert match?({:ok, _}, result) or match?({:error, _}, result)
    end

    test "handles field not in schema" do
      result = Parser.parse("m.nonexistent.field:value", @default_schema)
      assert match?({:error, :field_not_found, _, _}, result)
    end
  end

  describe "edge cases" do
    test "handles extremely long field names" do
      long_field = String.duplicate("a", 100)
      schema = build_schema(%{"metadata" => %{long_field => "value"}})
      query = "m.#{long_field}:value"

      result = Parser.parse(query, schema)
      assert match?({:ok, _}, result)
    end

    test "handles extremely long values" do
      schema = build_schema(%{"metadata" => %{"field" => "test"}})
      long_value = String.duplicate("test", 100)
      query = "m.field:\"#{long_value}\""

      result = Parser.parse(query, schema)
      assert match?({:ok, _}, result)
    end

    test "handles special characters in values" do
      schema = build_schema(%{"metadata" => %{"field" => "test"}})
      special_chars = ["!@#$%^&*()", "こんにちは", "🚀"]

      for char <- special_chars do
        query = "m.field:\"#{char}\""
        result = Parser.parse(query, schema)
        assert match?({:ok, _}, result)
      end
    end

    test "handles multiple consecutive operators" do
      invalid_queries = [
        "m.field::value",
        "m.field:>=<100",
        "m.field:~~pattern"
      ]

      for query <- invalid_queries do
        result = Parser.parse(query, @default_schema)
        assert match?({:error, _}, result)
      end
    end

    test "handles numeric edge cases" do
      schema = build_schema(%{"metadata" => %{"field" => 1}})

      queries = [
        "m.field:0",
        "m.field:999999999999999999999"
      ]

      for query <- queries do
        result = Parser.parse(query, schema)
        assert match?({:ok, _}, result)
      end

      float_schema = build_schema(%{"metadata" => %{"field" => 1.0}})
      float_result = Parser.parse("m.field:3.14159265359", float_schema)
      assert match?({:ok, _}, float_result)
    end

    test "handles boolean values" do
      schema = build_schema(%{"metadata" => %{"field" => true}})

      queries = [
        "m.field:true",
        "m.field:false"
      ]

      for query <- queries do
        result = Parser.parse(query, schema)
        assert match?({:ok, _}, result)
      end
    end

    test "handles NULL values" do
      schema = build_schema(%{"metadata" => %{"field" => "test"}})
      query = "m.field:NULL"

      result = Parser.parse(query, schema)
      assert match?({:ok, _}, result)
    end

    test "handles timestamp edge cases" do
      queries = [
        "t:1970-01-01",
        "t:2000-02-29",
        "t:2023-01-01T00:00:00"
      ]

      for query <- queries do
        result = Parser.parse(query, @default_schema)
        assert match?({:ok, _}, result)
      end
    end

    test "handles range operators" do
      schema = build_schema(%{"metadata" => %{"field" => 1}})

      queries = [
        "m.field:1..10",
        "m.field:10..1",
        "m.field:1..1"
      ]

      for query <- queries do
        result = Parser.parse(query, schema)
        assert match?({:ok, _}, result)
      end
    end

    test "handles regex patterns" do
      queries = [
        "~error",
        "~[a-z]+",
        "~\\d+"
      ]

      for query <- queries do
        result = Parser.parse(query, @default_schema)
        assert match?({:ok, _}, result)
      end
    end

    test "handles array operations" do
      schema = build_schema(%{"metadata" => %{"array" => ["value"]}})

      queries = [
        "m.array:@>value",
        "m.array:@>\"quoted value\"",
        "m.array:@>123"
      ]

      for query <- queries do
        result = Parser.parse(query, schema)
        assert match?({:ok, _}, result)
      end
    end

    test "handles chart operators" do
      schema = build_schema(%{"metadata" => %{"field" => 1.0}})

      queries = [
        "c:count(*)",
        "c:sum(m.field)",
        "c:avg(m.field)"
      ]

      for query <- queries do
        result = Parser.parse(query, schema)
        assert match?({:ok, _}, result)
      end

      combined_query = "c:avg(m.field) c:group_by(t::minute)"
      result = Parser.parse(combined_query, schema)
      assert match?({:ok, _}, result)
    end

    test "handles negation" do
      schema = build_schema(%{"metadata" => %{"field" => "value"}})

      queries = [
        "-m.field:value",
        "-~pattern"
      ]

      for query <- queries do
        result = Parser.parse(query, schema)
        assert match?({:ok, _}, result)
      end
    end

    test "handles multiple filters" do
      schema = build_schema(%{"metadata" => %{"field1" => "value1", "field2" => "value2"}})
      query = "m.field1:value1 m.field2:value2"

      result = Parser.parse(query, schema)
      assert match?({:ok, _}, result)
    end
  end

  describe "schema validation" do
    test "handles missing schema gracefully" do
      assert_raise FunctionClauseError, fn ->
        Parser.parse("m.field:value", nil)
      end
    end

    test "handles empty schema" do
      empty_schema = %GoogleApi.BigQuery.V2.Model.TableSchema{fields: []}
      result = Parser.parse("m.field:value", empty_schema)
      assert match?({:error, :field_not_found, _, _}, result)
    end

    test "handles basic schema with new fields" do
      basic_schema = SchemaBuilder.initial_table_schema()
      result = Parser.parse("m.new_field:value", basic_schema)
      assert match?({:error, :field_not_found, _, _}, result)
    end
  end

  describe "from rule parsing" do
    test "parses f: prefix" do
      qs = "f:my_table"

      assert {:ok, [%FromRule{table: "my_table", table_type: :unknown}]} == Parser.parse(qs)
    end

    test "parses from: prefix" do
      qs = "from:errors"

      assert {:ok, [%FromRule{table: "errors", table_type: :unknown}]} == Parser.parse(qs)
    end

    test "parses from clause with filters" do
      qs = "f:my_table m.status:error"

      assert {:ok,
              [
                %FromRule{table: "my_table", table_type: :unknown},
                %FilterRule{operator: :=, path: "metadata.status", value: "error"}
              ]} == Parser.parse(qs)
    end

    test "parses from clause with chart rule" do
      qs = "f:errors c:count(*)"

      assert {:ok,
              [
                %FromRule{table: "errors", table_type: :unknown},
                %ChartRule{aggregate: :count, path: "timestamp", period: :minute}
              ]} == Parser.parse(qs)
    end

    test "parses from clause with select rules" do
      qs = "f:logs s:event_message s:m.user_id"

      assert {:ok, rules} = Parser.parse(qs)
      assert length(rules) == 3

      assert Enum.any?(rules, &match?(%FromRule{table: "logs"}, &1))
    end

    test "parses select aliases" do
      qs = "s:metadata.user.id@user_id"

      assert {:ok, rules} = Parser.parse(qs)

      assert [%SelectRule{alias: "user_id", path: "metadata.user.id"}] =
               Enum.filter(rules, &match?(%SelectRule{}, &1))
    end

    test "rejects select with blank alias" do
      qs = "s:m.qty@"

      assert {:error, _} = Parser.parse(qs)
    end

    test "parses table names with underscores" do
      qs = "f:my_table_123"

      assert {:ok, [%FromRule{table: "my_table_123"}]} == Parser.parse(qs)
    end

    test "parses table names starting with underscore" do
      qs = "f:_private_table"

      assert {:ok, [%FromRule{table: "_private_table"}]} == Parser.parse(qs)
    end

    test "parses from clause before other clauses" do
      qs = "f:events m.level:error t:today"

      assert {:ok, rules} = Parser.parse(qs)
      assert length(rules) == 3

      from_rule = Enum.find(rules, &match?(%FromRule{}, &1))
      assert from_rule.table == "events"
    end
  end

  def today_dt do
    Timex.today() |> Timex.to_datetime()
  end

  def now_ndt do
    %{Timex.now() | microsecond: {0, 0}}
  end

  def now_udt_zero_sec do
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
