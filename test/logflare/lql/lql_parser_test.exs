defmodule Logflare.LqlParserTest do
  @moduledoc false
  use Logflare.DataCase, async: true
  alias Logflare.Lql
  alias Logflare.Lql.Parser, as: Parser
  alias Logflare.Lql.{Utils, ChartRule, FilterRule}
  alias Logflare.DateTimeUtils
  alias Logflare.Source.BigQuery.SchemaBuilder

  @default_schema Logflare.BigQuery.TableSchema.SchemaBuilderHelpers.schemas().initial

  describe "LQL parsing" do
    test "word string regexp" do
      schema = SchemaBuilder.build_table_schema(%{}, @default_schema)
      str = ~S|~user ~sign ~up|
      {:ok, result} = Parser.parse(str, schema)

      lql_rules = [
        %FilterRule{operator: :"~", path: "event_message", value: "user", modifiers: %{}},
        %FilterRule{operator: :"~", path: "event_message", value: "sign", modifiers: %{}},
        %FilterRule{operator: :"~", path: "event_message", value: "up", modifiers: %{}}
      ]

      assert Utils.get_filter_rules(result) == lql_rules
      assert Lql.encode!(lql_rules) == str
    end

    test "quoted string regexp" do
      schema = SchemaBuilder.build_table_schema(%{}, @default_schema)
      str = ~S|~new ~"user sign up" ~server|
      {:ok, result} = Parser.parse(str, schema)

      lql_rules = [
        %FilterRule{operator: :"~", path: "event_message", value: "new", modifiers: %{}},
        %FilterRule{
          operator: :"~",
          path: "event_message",
          value: "user sign up",
          modifiers: %{quoted_string: true}
        },
        %FilterRule{
          operator: :"~",
          path: "event_message",
          value: "server",
          modifiers: %{}
        }
      ]

      assert Utils.get_filter_rules(result) == lql_rules
      assert Lql.encode!(lql_rules) == str
    end

    test "word contains" do
      schema = SchemaBuilder.build_table_schema(%{}, @default_schema)
      str = ~S|user sign up|
      {:ok, result} = Parser.parse(str, schema)

      lql_rules = [
        %FilterRule{
          operator: :string_contains,
          path: "event_message",
          value: "user",
          modifiers: %{}
        },
        %FilterRule{
          operator: :string_contains,
          path: "event_message",
          value: "sign",
          modifiers: %{}
        },
        %FilterRule{
          operator: :string_contains,
          path: "event_message",
          value: "up",
          modifiers: %{}
        }
      ]

      assert Utils.get_filter_rules(result) == lql_rules
      assert Lql.encode!(lql_rules) == str
    end

    test "word contains with allowed characters" do
      str = ~S|$user! sign.up user_sign_up%|
      {:ok, result} = Parser.parse(str, @default_schema)

      lql_rules = [
        %FilterRule{
          modifiers: %{},
          operator: :string_contains,
          path: "event_message",
          shorthand: nil,
          value: "$user!",
          values: nil
        },
        %FilterRule{
          modifiers: %{},
          operator: :string_contains,
          path: "event_message",
          shorthand: nil,
          value: "sign.up",
          values: nil
        },
        %FilterRule{
          modifiers: %{},
          operator: :string_contains,
          path: "event_message",
          shorthand: nil,
          value: "user_sign_up%",
          values: nil
        }
      ]

      assert Utils.get_filter_rules(result) == lql_rules
      assert Lql.encode!(lql_rules) == str
    end

    test "quoted string contains" do
      schema = SchemaBuilder.build_table_schema(%{}, @default_schema)
      str = ~S|new "user sign up" server|
      {:ok, result} = Parser.parse(str, schema)

      lql_rules = [
        %FilterRule{
          operator: :string_contains,
          path: "event_message",
          value: "new",
          modifiers: %{}
        },
        %FilterRule{
          operator: :string_contains,
          path: "event_message",
          value: "user sign up",
          modifiers: %{quoted_string: true}
        },
        %FilterRule{
          operator: :string_contains,
          path: "event_message",
          value: "server",
          modifiers: %{}
        }
      ]

      assert Utils.get_filter_rules(result) == lql_rules
      assert Lql.encode!(lql_rules) == str
    end

    @schema SchemaBuilder.build_table_schema(
              %{
                user: %{
                  type: "string",
                  id: 1,
                  views: 1,
                  about: "string"
                },
                users: %{source_count: 100},
                context: %{
                  error_count3: 100.0,
                  error_count2: 100.0,
                  error_count4: 100.0,
                  error_count1: 100.0,
                  error_count: 100.0
                }
              }
              |> MapKeys.to_strings(),
              @default_schema
            )

    test "metadata quoted string value" do
      str = ~S|
       metadata.user.type:"string"
       metadata.user.type:"string string"
       metadata.user.type:~"string"
       metadata.user.type:~"string string"
       |
      {:ok, result} = Parser.parse(str, @schema)

      lql_rules = [
        %FilterRule{
          modifiers: %{quoted_string: true},
          operator: :=,
          path: "metadata.user.type",
          value: "string"
        },
        %FilterRule{
          modifiers: %{quoted_string: true},
          operator: :=,
          path: "metadata.user.type",
          value: "string string"
        },
        %FilterRule{
          modifiers: %{quoted_string: true},
          operator: :"~",
          path: "metadata.user.type",
          value: "string"
        },
        %FilterRule{
          modifiers: %{quoted_string: true},
          operator: :"~",
          path: "metadata.user.type",
          value: "string string"
        }
      ]

      assert Utils.get_filter_rules(result) == lql_rules
      assert Lql.encode!(lql_rules) == clean_and_trim_lql_string(str)
    end

    test "range value" do
      str = ~S|
       metadata.context.error_count1:30.1..300.1
       metadata.context.error_count2:40.1..400.1
       metadata.context.error_count3:20.1..200.1
       metadata.context.error_count4:0.1..0.9
       metadata.users.source_count:50..200
       |
      {:ok, result} = Parser.parse(str, @schema)

      lql_rules = [
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :range,
          path: "metadata.context.error_count1",
          shorthand: nil,
          value: nil,
          values: [30.1, 300.1]
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :range,
          path: "metadata.context.error_count2",
          shorthand: nil,
          value: nil,
          values: [40.1, 400.1]
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :range,
          path: "metadata.context.error_count3",
          shorthand: nil,
          value: nil,
          values: [20.1, 200.1]
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :range,
          path: "metadata.context.error_count4",
          shorthand: nil,
          value: nil,
          values: [0.1, 0.9]
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :range,
          path: "metadata.users.source_count",
          shorthand: nil,
          value: nil,
          values: [50, 200]
        }
      ]

      assert Utils.get_filter_rules(result) == lql_rules
      assert Lql.encode!(lql_rules) == clean_and_trim_lql_string(str)
    end

    test "nested fields filter" do
      schema =
        SchemaBuilder.build_table_schema(
          %{
            user: %{
              type: "string",
              id: 1,
              views: 1,
              about: "string"
            },
            users: %{source_count: 100},
            context: %{error_count: 100}
          }
          |> MapKeys.to_strings(),
          @default_schema
        )

      str = ~S|
        metadata.context.error_count:>=100
        metadata.user.about:~referrall
        metadata.user.id:<1
        metadata.user.type:paid
        metadata.user.views:<=1
        metadata.users.source_count:>100
        t:2019-01-01T00:13:37..2019-02-01T00:23:34
      |

      {:ok, result} = Parser.parse(str, schema)

      lql_rules = [
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :>=,
          path: "metadata.context.error_count",
          value: 100
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :"~",
          path: "metadata.user.about",
          value: "referrall"
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :<,
          path: "metadata.user.id",
          value: 1
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :=,
          path: "metadata.user.type",
          value: "paid"
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :<=,
          path: "metadata.user.views",
          value: 1
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :>,
          path: "metadata.users.source_count",
          value: 100
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :range,
          path: "timestamp",
          shorthand: nil,
          value: nil,
          values: [~N[2019-01-01 00:13:37Z], ~N[2019-02-01 00:23:34Z]]
        }
      ]

      assert lql_rules == Utils.get_filter_rules(result)

      assert Lql.encode!(lql_rules) ==
               "m.context.error_count:>=100 m.user.about:~referrall m.user.id:<1 m.user.type:paid m.user.views:<=1 m.users.source_count:>100 t:2019-{01..02}-01T00:{13..23}:{37..34}"

      str = ~S|
        t:2019-01-01..2019-02-01
      |

      lql_rules = [
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :range,
          path: "timestamp",
          shorthand: nil,
          value: nil,
          values: [~D[2019-01-01], ~D[2019-02-01]]
        }
      ]

      {:ok, result} = Parser.parse(str, schema)

      assert Utils.get_filter_rules(result) == lql_rules

      assert Lql.encode!(lql_rules) == clean_and_trim_lql_string(str)
    end

    test "nested fields filter 2" do
      schema =
        SchemaBuilder.build_table_schema(
          %{
            context: %{
              file: "string",
              line_number: 1
            },
            user: %{group_id: 5, admin: false},
            log: %{
              label1: "string",
              metric1: 10,
              metric2: 10,
              metric3: 10,
              metric4: 10
            }
          }
          |> MapKeys.to_strings(),
          @default_schema
        )

      str = ~S|
         log "was generated" "by logflare pinger"
         metadata.context.file:"some module.ex"
         metadata.context.line_number:100
         metadata.log.label1:~origin
         metadata.log.metric1:<10
         metadata.log.metric2:<=10
         metadata.log.metric3:>10
         metadata.log.metric4:>=10
         metadata.user.admin:false
         metadata.user.group_id:5
         c:count(metadata.log.metric4)
         c:group_by(t::minute)
       |

      {:ok, lql_rules} = Parser.parse(str, schema)

      filters = [
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :string_contains,
          path: "event_message",
          value: "log"
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{quoted_string: true},
          operator: :string_contains,
          path: "event_message",
          value: "was generated"
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{quoted_string: true},
          operator: :string_contains,
          path: "event_message",
          value: "by logflare pinger"
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{quoted_string: true},
          operator: :=,
          path: "metadata.context.file",
          value: "some module.ex"
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :=,
          path: "metadata.context.line_number",
          value: 100
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :"~",
          path: "metadata.log.label1",
          value: "origin"
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :<,
          path: "metadata.log.metric1",
          value: 10
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :<=,
          path: "metadata.log.metric2",
          value: 10
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :>,
          path: "metadata.log.metric3",
          value: 10
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :>=,
          path: "metadata.log.metric4",
          value: 10
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :=,
          path: "metadata.user.admin",
          value: false
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :=,
          path: "metadata.user.group_id",
          value: 5
        }
      ]

      assert Utils.get_filter_rules(lql_rules) == filters

      assert Utils.get_chart_rules(lql_rules) == [
               %ChartRule{
                 path: "metadata.log.metric4",
                 value_type: :integer
               }
             ]

      assert Lql.encode!(lql_rules) == clean_and_trim_lql_string(str)
    end

    test "negated filter" do
      schema =
        SchemaBuilder.build_table_schema(
          %{
            user: %{
              type: "string",
              id: 1,
              views: 1,
              about: "string"
            },
            users: %{source_count: 100},
            context: %{error_count: 100}
          }
          |> MapKeys.to_strings(),
          @default_schema
        )

      str = ~S|
        -metadata.context.error_count:>=100
        -metadata.user.about:~referrall
        -metadata.user.type:paid
        -t:2019-01-01T00:13:37..2019-02-01T00:23:34
      |

      {:ok, result} = Parser.parse(str, schema)

      lql_rules = [
        %Logflare.Lql.FilterRule{
          modifiers: %{negate: true},
          operator: :>=,
          path: "metadata.context.error_count",
          value: 100
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{negate: true},
          operator: :"~",
          path: "metadata.user.about",
          value: "referrall"
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{negate: true},
          operator: :=,
          path: "metadata.user.type",
          value: "paid"
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{negate: true},
          operator: :range,
          path: "timestamp",
          shorthand: nil,
          value: nil,
          values: [~N[2019-01-01 00:13:37Z], ~N[2019-02-01 00:23:34Z]]
        }
      ]

      assert lql_rules == Utils.get_filter_rules(result)

      assert "-m.context.error_count:>=100 -m.user.about:~referrall -m.user.type:paid -t:2019-{01..02}-01T00:{13..23}:{37..34}" ==
               Lql.encode!(lql_rules)

      str = ~S|
        t:2019-01-01..2019-02-01
      |

      lql_rules = [
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :range,
          path: "timestamp",
          shorthand: nil,
          value: nil,
          values: [~D[2019-01-01], ~D[2019-02-01]]
        }
      ]

      {:ok, result} = Parser.parse(str, schema)

      assert Utils.get_filter_rules(result) == lql_rules

      assert Lql.encode!(lql_rules) == clean_and_trim_lql_string(str)
    end

    @schema SchemaBuilder.build_table_schema(
              %{
                context: %{
                  file: "string",
                  address: "string",
                  line_number: 1
                },
                user: %{group_id: 5, cluster_id: 100, admin: false},
                log: %{
                  label1: "string",
                  metric1: 10,
                  metric2: 10,
                  metric3: 10,
                  metric4: 10
                }
              }
              |> MapKeys.to_strings(),
              @default_schema
            )

    test "nested fields filter with timestamp 3" do
      str = ~S|
      log "was generated" "by logflare pinger" error
      metadata.context.address:~"\\d\\d\\d ST"
      metadata.context.file:"some module.ex"
      metadata.context.line_number:100
      metadata.log.metric1:<10
      metadata.log.metric4:<10
      metadata.user.cluster_id:200..300
      metadata.user.group_id:5
      t:>2019-01-01
      t:<=2019-04-20
      t:<2020-01-01T03:14:15
      t:>=2019-01-01T03:14:15
      t:<=2010-04-20|

      {:ok, result} = Parser.parse(str, @schema)

      expected = [
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :string_contains,
          path: "event_message",
          shorthand: nil,
          value: "log",
          values: nil
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{quoted_string: true},
          operator: :string_contains,
          path: "event_message",
          shorthand: nil,
          value: "was generated",
          values: nil
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{quoted_string: true},
          operator: :string_contains,
          path: "event_message",
          shorthand: nil,
          value: "by logflare pinger",
          values: nil
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :string_contains,
          path: "event_message",
          shorthand: nil,
          value: "error",
          values: nil
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{quoted_string: true},
          operator: :"~",
          path: "metadata.context.address",
          shorthand: nil,
          value: "\\\\d\\\\d\\\\d ST",
          values: nil
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{quoted_string: true},
          operator: :=,
          path: "metadata.context.file",
          shorthand: nil,
          value: "some module.ex",
          values: nil
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :=,
          path: "metadata.context.line_number",
          shorthand: nil,
          value: 100,
          values: nil
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :<,
          path: "metadata.log.metric1",
          shorthand: nil,
          value: 10,
          values: nil
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :<,
          path: "metadata.log.metric4",
          shorthand: nil,
          value: 10,
          values: nil
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :range,
          path: "metadata.user.cluster_id",
          shorthand: nil,
          value: nil,
          values: [200, 300]
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :=,
          path: "metadata.user.group_id",
          shorthand: nil,
          value: 5,
          values: nil
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :>,
          path: "timestamp",
          shorthand: nil,
          value: ~D[2019-01-01],
          values: nil
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :<=,
          path: "timestamp",
          shorthand: nil,
          value: ~D[2019-04-20],
          values: nil
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :<,
          path: "timestamp",
          shorthand: nil,
          value: ~N[2020-01-01 03:14:15Z],
          values: nil
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :>=,
          path: "timestamp",
          shorthand: nil,
          value: ~N[2019-01-01 03:14:15Z],
          values: nil
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :<=,
          path: "timestamp",
          shorthand: nil,
          value: ~D[2010-04-20],
          values: nil
        }
      ]

      assert Utils.get_filter_rules(result) == expected

      assert length(Utils.get_filter_rules(result)) == length(expected)

      assert Lql.encode!(result) ==
               clean_and_trim_lql_string(str) |> String.replace(" metadata.", " m.")
    end

    test "timestamp with microseconds" do
      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: %{},
                  operator: :>=,
                  path: "timestamp",
                  shorthand: nil,
                  value: ~N[2020-01-01 00:00:00.345000Z]
                }
              ]} == Parser.parse("timestamp:>=2020-01-01T00:00:00.345000", @schema)
    end

    test "timestamp shorthands" do
      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: %{},
                  operator: :=,
                  path: "timestamp",
                  shorthand: "now",
                  value: now_ndt()
                }
              ]} == Parser.parse("timestamp:now", @schema)

      lvalue = Timex.today() |> Timex.to_datetime()

      rvalue =
        Timex.today()
        |> Timex.shift(days: 1)
        |> Timex.to_datetime()
        |> Timex.shift(seconds: -1)

      assert {
               :ok,
               [
                 %Logflare.Lql.FilterRule{
                   modifiers: %{},
                   operator: :range,
                   path: "timestamp",
                   shorthand: "today",
                   value: nil,
                   values: [lvalue, rvalue]
                 }
               ]
             } == Parser.parse("timestamp:today", @schema)

      lvalue = Timex.today() |> Timex.shift(days: -1) |> Timex.to_datetime()
      rvalue = Timex.today() |> Timex.to_datetime() |> Timex.shift(seconds: -1)

      assert {
               :ok,
               [
                 %Logflare.Lql.FilterRule{
                   modifiers: %{},
                   operator: :range,
                   path: "timestamp",
                   shorthand: "yesterday",
                   value: nil,
                   values: [lvalue, rvalue]
                 }
               ]
             } == Parser.parse("timestamp:yesterday", @schema)

      lvalue = now_udt_zero_sec()
      rvalue = DateTimeUtils.truncate(Timex.now(), :second)

      assert {
               :ok,
               [
                 %Logflare.Lql.FilterRule{
                   modifiers: %{},
                   operator: :range,
                   path: "timestamp",
                   shorthand: "this@minute",
                   value: nil,
                   values: [lvalue, rvalue]
                 }
               ]
             } == Parser.parse("timestamp:this@minute", @schema)

      lvalue = %{now_udt_zero_sec() | minute: 0}

      assert {
               :ok,
               [
                 %Logflare.Lql.FilterRule{
                   modifiers: %{},
                   operator: :range,
                   path: "timestamp",
                   shorthand: "this@hour",
                   value: nil,
                   values: [lvalue, rvalue]
                 }
               ]
             } == Parser.parse("timestamp:this@hour", @schema)

      lvalue = %{now_udt_zero_sec() | minute: 0, hour: 0}

      assert {
               :ok,
               [
                 %Logflare.Lql.FilterRule{
                   modifiers: %{},
                   operator: :range,
                   path: "timestamp",
                   shorthand: "this@day",
                   value: nil,
                   values: [lvalue, rvalue]
                 }
               ]
             } == Parser.parse("timestamp:this@day", @schema)

      lvalue = Timex.beginning_of_week(%{now_udt_zero_sec() | minute: 0, hour: 0})

      assert {
               :ok,
               [
                 %Logflare.Lql.FilterRule{
                   modifiers: %{},
                   operator: :range,
                   path: "timestamp",
                   shorthand: "this@week",
                   value: nil,
                   values: [lvalue, rvalue]
                 }
               ]
             } == Parser.parse("timestamp:this@week", @schema)

      lvalue = Timex.beginning_of_month(%{now_udt_zero_sec() | minute: 0, hour: 0})

      assert {
               :ok,
               [
                 %Logflare.Lql.FilterRule{
                   modifiers: %{},
                   operator: :range,
                   path: "timestamp",
                   shorthand: "this@month",
                   value: nil,
                   values: [lvalue, rvalue]
                 }
               ]
             } == Parser.parse("timestamp:this@month", @schema)

      lvalue = Timex.beginning_of_year(%{now_udt_zero_sec() | minute: 0, hour: 0})

      assert {
               :ok,
               [
                 %Logflare.Lql.FilterRule{
                   modifiers: %{},
                   operator: :range,
                   path: "timestamp",
                   shorthand: "this@year",
                   value: nil,
                   values: [lvalue, rvalue]
                 }
               ]
             } == Parser.parse("timestamp:this@year", @schema)

      lvalue = Timex.shift(now_ndt(), seconds: -50)
      rvalue = now_ndt()

      assert {
               :ok,
               [
                 %Logflare.Lql.FilterRule{
                   modifiers: %{},
                   operator: :range,
                   path: "timestamp",
                   shorthand: "last@50second",
                   value: nil,
                   values: [lvalue, rvalue]
                 }
               ]
             } == Parser.parse("timestamp:last@50s", @schema)

      lvalue = Timex.shift(now_udt_zero_sec(), minutes: -43)

      assert {
               :ok,
               [
                 %Logflare.Lql.FilterRule{
                   modifiers: %{},
                   operator: :range,
                   path: "timestamp",
                   shorthand: "last@43minute",
                   value: nil,
                   values: [lvalue, rvalue]
                 }
               ]
             } == Parser.parse("timestamp:last@43m", @schema)

      lvalue = Timex.shift(%{now_udt_zero_sec() | minute: 0}, hours: -100)

      assert {
               :ok,
               [
                 %Logflare.Lql.FilterRule{
                   modifiers: %{},
                   operator: :range,
                   path: "timestamp",
                   shorthand: "last@100hour",
                   value: nil,
                   values: [lvalue, rvalue]
                 }
               ]
             } == Parser.parse("timestamp:last@100h", @schema)

      lvalue = Timex.shift(%{now_udt_zero_sec() | minute: 0, hour: 0}, days: -7)

      assert {
               :ok,
               [
                 %Logflare.Lql.FilterRule{
                   modifiers: %{},
                   operator: :range,
                   path: "timestamp",
                   shorthand: "last@7day",
                   value: nil,
                   values: [lvalue, rvalue]
                 }
               ]
             } == Parser.parse("timestamp:last@7d", @schema)

      lvalue = Timex.shift(%{now_udt_zero_sec() | minute: 0, hour: 0}, weeks: -2)

      assert {
               :ok,
               [
                 %Logflare.Lql.FilterRule{
                   modifiers: %{},
                   operator: :range,
                   path: "timestamp",
                   shorthand: "last@2week",
                   value: nil,
                   values: [lvalue, rvalue]
                 }
               ]
             } == Parser.parse("timestamp:last@2w", @schema)

      lvalue = Timex.shift(%{now_udt_zero_sec() | minute: 0, hour: 0}, months: -1)

      lql_rules = [
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :range,
          path: "timestamp",
          shorthand: "last@1month",
          value: nil,
          values: [lvalue, rvalue]
        }
      ]

      str = "timestamp:last@1mm"

      assert {:ok, lql_rules} == Parser.parse(str, @schema)

      # assert clean_and_trim_lql_string(str) == Lql.encode!(lql_rules)

      lvalue = Timex.shift(%{now_udt_zero_sec() | minute: 0, hour: 0}, years: -1)

      assert {
               :ok,
               [
                 %Logflare.Lql.FilterRule{
                   modifiers: %{},
                   operator: :range,
                   path: "timestamp",
                   shorthand: "last@1year",
                   value: nil,
                   values: [lvalue, rvalue]
                 }
               ]
             } == Parser.parse("timestamp:last@1y", @schema)
    end

    test "timestamp range shorthand" do
      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: %{},
                  operator: :range,
                  path: "timestamp",
                  shorthand: nil,
                  value: nil,
                  values: [~N[2020-01-01 00:00:00Z], ~N[2020-01-01 00:50:00Z]]
                }
              ]} == Parser.parse("timestamp:2020-01-01T00:{00..50}:00Z", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: %{},
                  operator: :range,
                  path: "timestamp",
                  shorthand: nil,
                  value: nil,
                  values: [
                    ~N[2020-01-01 00:00:35Z],
                    ~N[2020-01-01 00:00:55Z]
                  ]
                }
              ]} == Parser.parse("timestamp:2020-01-01T00:00:{35..55}", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: %{},
                  operator: :range,
                  path: "timestamp",
                  shorthand: nil,
                  value: nil,
                  values: [
                    ~N[2020-05-01 12:44:24Z],
                    ~N[2020-06-01 12:44:24Z]
                  ]
                }
              ]} == Parser.parse("timestamp:2020-{05..06}-01T12:44:24", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: %{},
                  operator: :range,
                  path: "timestamp",
                  shorthand: nil,
                  value: nil,
                  values: [
                    ~N[2020-05-01 12:00:05Z],
                    ~N[2020-05-01 14:00:05Z]
                  ]
                }
              ]} == Parser.parse("timestamp:2020-05-01T{12..14}:00:05Z", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: %{},
                  operator: :range,
                  path: "timestamp",
                  shorthand: nil,
                  value: nil,
                  values: [
                    ~N[2020-05-01 14:00:05.000001Z],
                    ~N[2020-05-01 14:00:05.460560Z]
                  ]
                }
              ]} == Parser.parse("timestamp:2020-05-01T14:00:05.{000001..460560}Z", @schema)
    end

    test "m,t shorthands" do
      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: %{},
                  operator: :=,
                  path: "metadata.user.cluster_id",
                  value: 1
                }
              ]} == Parser.parse("m.user.cluster_id:1", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: %{},
                  operator: :=,
                  path: "timestamp",
                  shorthand: "now",
                  value: now_ndt()
                }
              ]} == Parser.parse("t:now", @schema)
    end

    @schema SchemaBuilder.build_table_schema(
              %{
                "nullable" => "string"
              },
              @default_schema
            )
    test "NULL" do
      str = ~S|
         metadata.nullable:NULL
       |

      {:ok, result} = Parser.parse(str, @schema)

      assert result == [
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.nullable",
                 value: :NULL
               }
             ]
    end

    @schema SchemaBuilder.build_table_schema(
              %{
                "level" => "info"
              },
              @default_schema
            )
    test "level ranges" do
      str = ~S|
         metadata.level:info..error
       |

      {:ok, result} = Parser.parse(str, @schema)

      assert result == [
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.level",
                 value: "info"
               },
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.level",
                 value: "warning"
               },
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.level",
                 value: "error"
               }
             ]

      str = ~S|
         metadata.level:debug..warning
       |

      {:ok, lql_rules} = Parser.parse(str, @schema)

      assert lql_rules == [
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.level",
                 value: "debug"
               },
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.level",
                 value: "info"
               },
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.level",
                 value: "warning"
               }
             ]

      # assert Lql.encode!(lql_rules) == clean_and_trim_lql_string(str)

      str = ~S|
         metadata.level:debug..error
       |

      {:ok, lql_rules} = Parser.parse(str, @schema)

      assert lql_rules == [
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.level",
                 value: "debug"
               },
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.level",
                 value: "info"
               },
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.level",
                 value: "warning"
               },
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.level",
                 value: "error"
               }
             ]

      # assert Lql.encode!(lql_rules) == clean_and_trim_lql_string(str)
    end

    @schema SchemaBuilder.build_table_schema(
              %{
                "string_array" => ["string1", "string2"],
                "integer_array" => [1, 2],
                "float_array" => [1.0, 2.0]
              },
              @default_schema
            )

    test "list contains operator: string" do
      lql_rules = [
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :list_includes,
          path: "metadata.string_array",
          value: "string"
        }
      ]

      str = "metadata.string_array:@>string"
      assert {:ok, lql_rules} == Parser.parse(str, @schema)
      assert clean_and_trim_lql_string(str) == Lql.encode!(lql_rules)

      lql_rules = [
        %Logflare.Lql.FilterRule{
          modifiers: %{quoted_string: true},
          operator: :list_includes,
          path: "metadata.string_array",
          value: "string"
        }
      ]

      str = "m.string_array:_includes(string)"
      assert {:ok, lql_rules} == Parser.parse(str, @schema)
    end

    test "list contains operator: integer" do
      filter = [
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :list_includes,
          path: "metadata.integer_array",
          value: 1
        }
      ]

      assert {:ok, filter} == Parser.parse("m.integer_array:@>1", @schema)

      filter = [
        %Logflare.Lql.FilterRule{
          modifiers: %{quoted_string: true},
          operator: :list_includes,
          path: "metadata.integer_array",
          value: 1
        }
      ]

      assert {:ok, filter} == Parser.parse("m.integer_array:_includes(1)", @schema)
    end

    test "list contains operator: float" do
      filter = [
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :list_includes,
          path: "metadata.float_array",
          value: 1.0
        }
      ]

      assert {:ok, filter} == Parser.parse("m.float_array:@>1.0", @schema)

      filter = [
        %Logflare.Lql.FilterRule{
          modifiers: %{quoted_string: true},
          operator: :list_includes,
          path: "metadata.float_array",
          value: 1.0
        }
      ]

      assert {:ok, filter} == Parser.parse("m.float_array:_includes(1.0)", @schema)
    end

    test "lt, lte, gte, gt for float values" do
      schema =
        SchemaBuilder.build_table_schema(
          %{
            log: %{
              metric5: 10.0
            },
            user: %{
              cluster_group: 1.0
            }
          }
          |> MapKeys.to_strings(),
          @default_schema
        )

      str = ~S|
         metadata.log.metric5:<10.0
         metadata.user.cluster_group:200.042..300.1337
       |

      {:ok, lql_rules} = Parser.parse(str, schema)

      assert Utils.get_filter_rules(lql_rules) == [
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :<,
                 path: "metadata.log.metric5",
                 shorthand: nil,
                 value: 10.0,
                 values: nil
               },
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :range,
                 path: "metadata.user.cluster_group",
                 shorthand: nil,
                 value: nil,
                 values: [200.042, 300.1337]
               }
             ]

      assert Lql.encode!(lql_rules) == clean_and_trim_lql_string(str)
    end

    test "chart period, chart aggregate" do
      schema =
        SchemaBuilder.build_table_schema(
          %{
            log: %{
              metric5: 10.0
            },
            user: %{
              cluster_group: 1.0
            }
          }
          |> MapKeys.to_strings(),
          @default_schema
        )

      str = ~S|
         c:sum(metadata.log.metric5)
         c:group_by(t::minute)
       |

      {:ok, result} = Parser.parse(str, schema)

      assert result == [
               %Logflare.Lql.ChartRule{
                 path: "metadata.log.metric5",
                 aggregate: :sum,
                 period: :minute,
                 value_type: :float
               }
             ]

      str = ~S|
         c:sum(m.log.metric5)
         c:group_by(t::minute)
       |

      {:ok, result} = Parser.parse(str, schema)

      assert result == [
               %Logflare.Lql.ChartRule{
                 path: "metadata.log.metric5",
                 aggregate: :sum,
                 period: :minute,
                 value_type: :float
               }
             ]

      assert Lql.encode!(result)
             |> String.replace("metadata", "m")
             |> String.replace("timestamp", "t") ==
               clean_and_trim_lql_string(str)
    end

    test "returns error on malformed timestamp filter" do
      schema =
        SchemaBuilder.build_table_schema(
          %{},
          @default_schema
        )

      str = ~S|
         log "was generated" "by logflare pinger"
         timestamp:>20
       |

      assert {:error,
              "Error while parsing timestamp filter value: expected ISO8601 string or range or shorthand, got '20'"} ==
               Parser.parse(str, schema)
    end

    test "suggests did you mean this when path is not present in schema" do
      schema =
        SchemaBuilder.build_table_schema(
          %{"user" => %{"user_id" => 1}},
          @default_schema
        )

      str = ~S|
        metadata.user.id:1
       |

      assert {
               :error,
               "LQL Parser error: path 'metadata.user.id' not present in source schema. Did you mean 'metadata.user.user_id'?"
             } == Parser.parse(str, schema)
    end

    test "returns human readable error for invalid query" do
      schema =
        SchemaBuilder.build_table_schema(
          %{},
          @default_schema
        )

      str = ~S|
         metadata.user.emailAddress:
         metadata.user.clusterId:200..300
       |

      assert {:error,
              "Error while parsing `metadata.user.emailAddress` field metadata filter value: \"\""} =
               Parser.parse(str, schema)
    end
  end

  describe "LQL parser for timestamp range shorthand" do
    test "simple case" do
      qs = "t:2020-{01..02}-01T00:{00..50}:00"

      lql_rules = [
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :range,
          path: "timestamp",
          shorthand: nil,
          value: nil,
          values: [~N[2020-01-01 00:00:00Z], ~N[2020-02-01 00:50:00Z]]
        }
      ]

      assert {:ok, lql_rules} == Parser.parse(qs, @default_schema)

      assert qs == Lql.encode!(lql_rules)

      lql_rules = [
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :range,
          path: "timestamp",
          shorthand: nil,
          value: nil,
          values: [
            ~N[2020-01-05 00:15:35],
            ~N[2020-12-30 23:20:55]
          ]
        }
      ]

      qs = "t:2020-{01..12}-{05..30}T{00..23}:{15..20}:{35..55}"

      assert {:ok, lql_rules} ==
               Parser.parse(
                 qs,
                 @schema
               )

      assert qs == Lql.encode!(lql_rules)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: %{},
                  operator: :range,
                  path: "timestamp",
                  shorthand: nil,
                  value: nil,
                  values: [
                    ~N[2020-01-01 00:15:35.000001],
                    ~N[2020-12-30 23:20:55.585444]
                  ]
                }
              ]} ==
               Parser.parse(
                 "timestamp:2020-{01..12}-{01..30}T{00..23}:{15..20}:{35..55}.{000001..585444}",
                 @schema
               )
    end
  end

  describe "LQL encoding" do
    test "to_datetime_with_range" do
      lv = "2020-01-01T03:14:15Z" |> NaiveDateTime.from_iso8601!()
      rv = "2020-01-01T03:54:15Z" |> NaiveDateTime.from_iso8601!()

      assert Lql.Encoder.to_datetime_with_range(lv, rv) == "2020-01-01T03:{14..54}:15"
    end

    test "to_datetime_with_range 2" do
      lv = "2020-01-01T03:40:15Z" |> NaiveDateTime.from_iso8601!()
      rv = "2020-01-01T03:45:15Z" |> NaiveDateTime.from_iso8601!()

      assert Lql.Encoder.to_datetime_with_range(lv, rv) == "2020-01-01T03:{40..45}:15"
    end

    test "to_datetime_with_range 3" do
      lv = "2020-01-01T03:05:15Z" |> NaiveDateTime.from_iso8601!()
      rv = "2020-01-01T03:59:15Z" |> NaiveDateTime.from_iso8601!()

      assert Lql.Encoder.to_datetime_with_range(lv, rv) == "2020-01-01T03:{05..59}:15"
    end

    test "to_datetime_with_range 4" do
      lv = "2020-01-01T17:00:15Z" |> NaiveDateTime.from_iso8601!()
      rv = "2020-01-01T23:00:15Z" |> NaiveDateTime.from_iso8601!()

      assert Lql.Encoder.to_datetime_with_range(lv, rv) == "2020-01-01T{17..23}:00:15"
    end

    test "to_datetime_with_range 5" do
      lv = "2020-01-01T17:00:15Z" |> NaiveDateTime.from_iso8601!()
      rv = "2020-01-15T17:00:15Z" |> NaiveDateTime.from_iso8601!()

      assert Lql.Encoder.to_datetime_with_range(lv, rv) == "2020-01-{01..15}T17:00:15"
    end

    test "to_datetime_with_range 6" do
      lv = "2020-12-01T17:00:15Z" |> NaiveDateTime.from_iso8601!()
      rv = "2020-12-01T17:00:15Z" |> NaiveDateTime.from_iso8601!()

      assert Lql.Encoder.to_datetime_with_range(lv, rv) == "2020-12-01T17:00:15"
    end

    test "to_datetime_with_range 7" do
      lv = "2020-12-01T17:00:15Z" |> NaiveDateTime.from_iso8601!()
      rv = "2020-12-01T17:50:15Z" |> NaiveDateTime.from_iso8601!()

      assert Lql.Encoder.to_datetime_with_range(lv, rv) == "2020-12-01T17:{00..50}:15"
    end
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
end
