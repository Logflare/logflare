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
    test "word string regexp with special characters" do
      schema = SchemaBuilder.build_table_schema(%{}, @default_schema)
      str = ~S|~\d\d ~^ ~^error$ ~up|
      {:ok, result} = Parser.parse(str, schema)

      lql_rules = [
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :"~",
          path: "event_message",
          shorthand: nil,
          value: "\\d\\d",
          values: nil
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :"~",
          path: "event_message",
          shorthand: nil,
          value: "^",
          values: nil
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :"~",
          path: "event_message",
          shorthand: nil,
          value: "^error$",
          values: nil
        },
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :"~",
          path: "event_message",
          shorthand: nil,
          value: "up",
          values: nil
        }
      ]

      assert Utils.get_filter_rules(result) == lql_rules
      assert Lql.encode!(lql_rules) == str
    end

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

      assert Lql.encode!(lql_rules) == "t:2019-{01..02}-01"
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

      assert Lql.encode!(lql_rules) == "t:2019-{01..02}-01"
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
      rvalue = DateTimeUtils.truncate(Timex.now(), :second)

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
      rvalue = DateTimeUtils.truncate(Timex.now(), :second)

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
      rvalue = DateTimeUtils.truncate(Timex.now(), :second)

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
      rvalue = DateTimeUtils.truncate(Timex.now(), :second)

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
      rvalue = DateTimeUtils.truncate(Timex.now(), :second)

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

      assert {
               :ok,
               [
                 %Logflare.Lql.FilterRule{
                   modifiers: %{},
                   operator: :range,
                   path: "timestamp",
                   shorthand: "last@50second",
                   value: nil,
                   values: [lvalue, now_ndt()]
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
                   values: [lvalue, now_ndt()]
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
                   values: [lvalue, now_ndt()]
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
                   values: [lvalue, now_ndt()]
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
                   values: [lvalue, now_ndt()]
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
          values: [lvalue, now_ndt()]
        }
      ]

      str = "timestamp:last@1mm"

      assert {:ok, lql_rules} == Parser.parse(str, @schema)

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
                   values: [lvalue, now_ndt()]
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

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: %{},
                  operator: :range,
                  path: "timestamp",
                  shorthand: nil,
                  value: nil,
                  values: [
                    ~N[2020-12-24 00:00:00],
                    ~N[2021-01-24 23:59:00]
                  ]
                }
              ]} == Parser.parse("t:{2020..2021}-{12..01}-24T{00..23}:{00..59}:00", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: %{},
                  operator: :range,
                  path: "timestamp",
                  shorthand: nil,
                  value: nil,
                  values: [~D[2020-12-24], ~D[2021-01-24]]
                }
              ]} == Parser.parse("t:{2020..2021}-{12..01}-24", @schema)
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
                 shorthand: nil,
                 value: "info",
                 values: nil
               },
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.level",
                 shorthand: nil,
                 value: "notice",
                 values: nil
               },
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.level",
                 shorthand: nil,
                 value: "warning",
                 values: nil
               },
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.level",
                 shorthand: nil,
                 value: "error",
                 values: nil
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
                 shorthand: nil,
                 value: "debug",
                 values: nil
               },
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.level",
                 shorthand: nil,
                 value: "info",
                 values: nil
               },
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.level",
                 shorthand: nil,
                 value: "notice",
                 values: nil
               },
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.level",
                 shorthand: nil,
                 value: "warning",
                 values: nil
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
                 shorthand: nil,
                 value: "debug",
                 values: nil
               },
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.level",
                 shorthand: nil,
                 value: "info",
                 values: nil
               },
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.level",
                 shorthand: nil,
                 value: "notice",
                 values: nil
               },
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.level",
                 shorthand: nil,
                 value: "warning",
                 values: nil
               },
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.level",
                 shorthand: nil,
                 value: "error",
                 values: nil
               }
             ]

      str = ~S|
         metadata.level:debug..critical
       |

      {:ok, lql_rules} = Parser.parse(str, @schema)

      assert lql_rules == [
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.level",
                 shorthand: nil,
                 value: "debug",
                 values: nil
               },
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.level",
                 shorthand: nil,
                 value: "info",
                 values: nil
               },
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.level",
                 shorthand: nil,
                 value: "notice",
                 values: nil
               },
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.level",
                 shorthand: nil,
                 value: "warning",
                 values: nil
               },
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.level",
                 shorthand: nil,
                 value: "error",
                 values: nil
               },
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.level",
                 shorthand: nil,
                 value: "critical",
                 values: nil
               }
             ]

      str = ~S|
         metadata.level:notice..warning
       |

      {:ok, lql_rules} = Parser.parse(str, @schema)

      assert lql_rules == [
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.level",
                 shorthand: nil,
                 value: "notice",
                 values: nil
               },
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.level",
                 shorthand: nil,
                 value: "warning",
                 values: nil
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

    test "boolean" do
      schema =
        SchemaBuilder.build_table_schema(
          %{
            isAllowed: true
          }
          |> MapKeys.to_strings(),
          @default_schema
        )

      str = ~S|
         metadata.isAllowed:true
       |

      {:ok, lql_rules} = Parser.parse(str, schema)

      assert Utils.get_filter_rules(lql_rules) == [
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.isAllowed",
                 shorthand: nil,
                 value: true,
                 values: nil
               }
             ]

      str = ~S|
         metadata.isAllowed:false
       |

      {:ok, lql_rules} = Parser.parse(str, schema)

      assert Utils.get_filter_rules(lql_rules) == [
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :=,
                 path: "metadata.isAllowed",
                 shorthand: nil,
                 value: false,
                 values: nil
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

    @tag :failing
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
               :field_not_found,
               "\n        metadata.user.user_id:1\n       ",
               [
                 "LQL Parser error: path 'metadata.user.id' not present in source schema. Did you mean '",
                 "metadata.user.user_id",
                 "'?"
               ]
             } = Parser.parse(str, schema)
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
    test "month/day range" do
      qs = "t:2020-{05..07}-01"

      lql_rules = [
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :range,
          path: "timestamp",
          shorthand: nil,
          value: nil,
          values: [~D[2020-05-01], ~D[2020-07-01]]
        }
      ]

      assert {:ok, lql_rules} == Parser.parse(qs, @default_schema)

      assert qs == Lql.encode!(lql_rules)

      qs = "t:2020-05-{01..02}"

      lql_rules = [
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :range,
          path: "timestamp",
          shorthand: nil,
          value: nil,
          values: [~D[2020-05-01], ~D[2020-05-02]]
        }
      ]

      assert {:ok, lql_rules} == Parser.parse(qs, @default_schema)

      assert qs == Lql.encode!(lql_rules)
    end

    test "timestamp filter with leading zero microseconds" do
      qs = "t:>2020-01-01T13:14:15.000500"

      lql_rules = [
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :>,
          path: "timestamp",
          shorthand: nil,
          value: ~N[2020-01-01 13:14:15.000500],
          values: nil
        }
      ]

      assert {:ok, lql_rules} == Parser.parse(qs, @default_schema)

      assert qs == Lql.encode!(lql_rules)
    end

    test "timestamp microsecond ranges" do
      qs = "t:2020-01-01T13:14:15.{0..515}"

      lql_rules = [
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :range,
          path: "timestamp",
          shorthand: nil,
          value: nil,
          values: [
            ~N[2020-01-01 13:14:15.000000],
            ~N[2020-01-01 13:14:15.515000]
          ]
        }
      ]

      assert {:ok, lql_rules} == Parser.parse(qs, @default_schema)

      assert qs == Lql.encode!(lql_rules)

      qs = "t:2020-01-01T13:14:15.{0101..3555}"

      lql_rules = [
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :range,
          path: "timestamp",
          shorthand: nil,
          value: nil,
          values: [
            ~N[2020-01-01 13:14:15.010100],
            ~N[2020-01-01 13:14:15.355500]
          ]
        }
      ]

      assert {:ok, lql_rules} == Parser.parse(qs, @default_schema)

      assert qs == Lql.encode!(lql_rules)

      qs = "t:2020-01-01T13:14:15.{1..7}"

      lql_rules = [
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :range,
          path: "timestamp",
          shorthand: nil,
          value: nil,
          values: [
            ~N[2020-01-01 13:14:15.100000],
            ~N[2020-01-01 13:14:15.700000]
          ]
        }
      ]

      assert {:ok, lql_rules} == Parser.parse(qs, @default_schema)

      assert qs == Lql.encode!(lql_rules)

      qs = "t:2020-01-01T13:14:15.{005001..100000}"

      lql_rules = [
        %Logflare.Lql.FilterRule{
          modifiers: %{},
          operator: :range,
          path: "timestamp",
          shorthand: nil,
          value: nil,
          values: [
            ~N[2020-01-01 13:14:15.005001],
            ~N[2020-01-01 13:14:15.100000]
          ]
        }
      ]

      assert {:ok, lql_rules} == Parser.parse(qs, @default_schema)

      assert "t:2020-01-01T13:14:15.{005001..1}" == Lql.encode!(lql_rules)
    end

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
