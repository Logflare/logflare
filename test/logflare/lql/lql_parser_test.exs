defmodule Logflare.LqlParserTest do
  @moduledoc false
  use Logflare.DataCase, async: true
  alias Logflare.Lql.Parser, as: Parser
  alias Logflare.Lql.Utils
  import Parser
  alias Logflare.Source.BigQuery.SchemaBuilder
  alias Logflare.Lql.ChartRule
  alias Logflare.Lql.FilterRule

  alias GoogleApi.BigQuery.V2.Model.TableFieldSchema, as: TFS
  @default_schema Logflare.BigQuery.TableSchema.SchemaBuilderHelpers.schemas().initial

  describe "LQL parsing for" do
    test "simple message search string" do
      schema = SchemaBuilder.build_table_schema(%{}, @default_schema)
      str = ~S|user sign up|
      {:ok, result} = Parser.parse(str, schema)

      assert Utils.get_filter_rules(result) == [
               %FilterRule{operator: :"~", path: "event_message", value: "sign", modifiers: []},
               %FilterRule{operator: :"~", path: "event_message", value: "up", modifiers: []},
               %FilterRule{operator: :"~", path: "event_message", value: "user", modifiers: []}
             ]
    end

    test "quoted message search string" do
      schema = SchemaBuilder.build_table_schema(%{}, @default_schema)
      str = ~S|new "user sign up" server|
      {:ok, result} = Parser.parse(str, schema)

      assert Utils.get_filter_rules(result) ==
               [
                 %FilterRule{operator: :"~", path: "event_message", value: "new", modifiers: []},
                 %FilterRule{
                   operator: :"~",
                   path: "event_message",
                   value: "server",
                   modifiers: []
                 },
                 %FilterRule{
                   operator: :"~",
                   path: "event_message",
                   value: "user sign up",
                   modifiers: []
                 }
               ]
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
                context: %{error_count: 100.0}
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

      assert Utils.get_filter_rules(result) ==
               [
                 %FilterRule{
                   modifiers: [],
                   operator: :=,
                   path: "metadata.user.type",
                   value: "string"
                 },
                 %FilterRule{
                   modifiers: [],
                   operator: :=,
                   path: "metadata.user.type",
                   value: "string string"
                 },
                 %FilterRule{
                   modifiers: [],
                   operator: :"~",
                   path: "metadata.user.type",
                   value: "string"
                 },
                 %FilterRule{
                   modifiers: [],
                   operator: :"~",
                   path: "metadata.user.type",
                   value: "string string"
                 }
               ]
    end

    test "range value" do
      str = ~S|
       metadata.users.source_count:50..200
       metadata.context.error_count:30.0..300
       metadata.context.error_count:40.0..400.0
       metadata.context.error_count:20.0..200
       metadata.context.error_count:0.1..0.9
       |
      {:ok, result} = Parser.parse(str, @schema)

      assert Utils.get_filter_rules(result) ==
               [
                 %FilterRule{
                   modifiers: [],
                   operator: :<=,
                   path: "metadata.context.error_count",
                   value: 0.9
                 },
                 %FilterRule{
                   modifiers: [],
                   operator: :<=,
                   path: "metadata.context.error_count",
                   value: 200.0
                 },
                 %FilterRule{
                   modifiers: [],
                   operator: :<=,
                   path: "metadata.context.error_count",
                   value: 300.0
                 },
                 %FilterRule{
                   modifiers: [],
                   operator: :<=,
                   path: "metadata.context.error_count",
                   value: 400.0
                 },
                 %FilterRule{
                   modifiers: [],
                   operator: :<=,
                   path: "metadata.users.source_count",
                   value: 200
                 },
                 %FilterRule{
                   modifiers: [],
                   operator: :>=,
                   path: "metadata.context.error_count",
                   value: 0.1
                 },
                 %FilterRule{
                   modifiers: [],
                   operator: :>=,
                   path: "metadata.context.error_count",
                   value: 20.0
                 },
                 %FilterRule{
                   modifiers: [],
                   operator: :>=,
                   path: "metadata.context.error_count",
                   value: 30.0
                 },
                 %FilterRule{
                   modifiers: [],
                   operator: :>=,
                   path: "metadata.context.error_count",
                   value: 40.0
                 },
                 %FilterRule{
                   modifiers: [],
                   operator: :>=,
                   path: "metadata.users.source_count",
                   value: 50
                 }
               ]
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
        metadata.user.type:paid
        metadata.user.id:<1
        metadata.user.views:<=1
        metadata.users.source_count:>100
        metadata.context.error_count:>=100
        metadata.user.about:~referrall
        timestamp:2019-01-01..2019-02-01
        timestamp:2019-01-01T00:13:37Z..2019-02-01T00:23:34Z
      |

      {:ok, result} = Parser.parse(str, schema)

      assert Utils.get_filter_rules(result) == [
               %Logflare.Lql.FilterRule{
                 modifiers: [],
                 operator: :<,
                 path: "metadata.user.id",
                 value: 1
               },
               %Logflare.Lql.FilterRule{
                 modifiers: [],
                 operator: :<=,
                 path: "metadata.user.views",
                 value: 1
               },
               %Logflare.Lql.FilterRule{
                 modifiers: [],
                 operator: :<=,
                 path: "timestamp",
                 value: ~D[2019-02-01]
               },
               %Logflare.Lql.FilterRule{
                 modifiers: [],
                 operator: :<=,
                 path: "timestamp",
                 value: ~U[2019-02-01 00:23:34Z]
               },
               %Logflare.Lql.FilterRule{
                 modifiers: [],
                 operator: :=,
                 path: "metadata.user.type",
                 value: "paid"
               },
               %Logflare.Lql.FilterRule{
                 modifiers: [],
                 operator: :>,
                 path: "metadata.users.source_count",
                 value: 100
               },
               %Logflare.Lql.FilterRule{
                 modifiers: [],
                 operator: :>=,
                 path: "metadata.context.error_count",
                 value: 100
               },
               %Logflare.Lql.FilterRule{
                 modifiers: [],
                 path: "timestamp",
                 operator: :>=,
                 value: ~D[2019-01-01]
               },
               %Logflare.Lql.FilterRule{
                 modifiers: [],
                 operator: :>=,
                 path: "timestamp",
                 value: ~U[2019-01-01 00:13:37Z]
               },
               %Logflare.Lql.FilterRule{
                 modifiers: [],
                 operator: :"~",
                 path: "metadata.user.about",
                 value: "referrall"
               }
             ]
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
         metadata.user.group_id:5
         metadata.user.admin:false
         metadata.log.label1:~origin
         metadata.log.metric1:<10
         metadata.log.metric2:<=10
         metadata.log.metric3:>10
         metadata.log.metric4:>=10
         chart:metadata.log.metric4
       |

      {:ok, result} = Parser.parse(str, schema)

      assert Utils.get_filter_rules(result) ==
               [
                 %FilterRule{
                   modifiers: [],
                   operator: :<,
                   path: "metadata.log.metric1",
                   value: 10
                 },
                 %FilterRule{
                   modifiers: [],
                   operator: :<=,
                   path: "metadata.log.metric2",
                   value: 10
                 },
                 %FilterRule{
                   modifiers: [],
                   operator: :=,
                   path: "metadata.context.file",
                   value: "some module.ex"
                 },
                 %FilterRule{
                   modifiers: [],
                   operator: :=,
                   path: "metadata.context.line_number",
                   value: 100
                 },
                 %FilterRule{
                   modifiers: [],
                   operator: :=,
                   path: "metadata.user.admin",
                   value: false
                 },
                 %FilterRule{
                   modifiers: [],
                   operator: :=,
                   path: "metadata.user.group_id",
                   value: 5
                 },
                 %FilterRule{
                   modifiers: [],
                   value: 10,
                   operator: :>,
                   path: "metadata.log.metric3"
                 },
                 %FilterRule{
                   modifiers: [],
                   operator: :>=,
                   path: "metadata.log.metric4",
                   value: 10
                 },
                 %FilterRule{
                   modifiers: [],
                   operator: :"~",
                   path: "event_message",
                   value: "by logflare pinger"
                 },
                 %FilterRule{modifiers: [], operator: :"~", path: "event_message", value: "log"},
                 %FilterRule{
                   modifiers: [],
                   operator: :"~",
                   path: "event_message",
                   value: "was generated"
                 },
                 %FilterRule{
                   modifiers: [],
                   operator: :"~",
                   path: "metadata.log.label1",
                   value: "origin"
                 }
               ]

      assert Utils.get_chart_rules(result) == [
               %ChartRule{
                 path: "metadata.log.metric4",
                 value_type: :integer
               }
             ]
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
         log "was generated" "by logflare pinger"
         timestamp:>2019-01-01
         timestamp:<=2019-04-20
         timestamp:<2020-01-01T03:14:15Z
         timestamp:>=2019-01-01T03:14:15Z
         metadata.context.file:"some module.ex"
         metadata.context.address:~"\d\d\d ST"
         metadata.context.line_number:100
         metadata.user.group_id:5
         metadata.user.cluster_id:200..300
         metadata.log.metric1:<10
         -metadata.log.metric4:<10
         -timestamp:<=2010-04-20
         -error
       |

      {:ok, result} = Parser.parse(str, @schema)

      expected =
        [
          %Logflare.Lql.FilterRule{
            modifiers: [],
            operator: :<,
            path: "metadata.log.metric1",
            value: 10
          },
          %Logflare.Lql.FilterRule{
            modifiers: [],
            operator: :<,
            path: "timestamp",
            value: ~U[2020-01-01 03:14:15Z]
          },
          %Logflare.Lql.FilterRule{
            modifiers: [],
            operator: :<=,
            path: "timestamp",
            value: ~D[2019-04-20]
          },
          %Logflare.Lql.FilterRule{
            modifiers: [],
            operator: :=,
            path: "metadata.context.file",
            value: "some module.ex"
          },
          %Logflare.Lql.FilterRule{
            modifiers: [],
            operator: :=,
            path: "metadata.context.line_number",
            value: 100
          },
          %Logflare.Lql.FilterRule{
            modifiers: [],
            operator: :=,
            path: "metadata.user.group_id",
            value: 5
          },
          %Logflare.Lql.FilterRule{
            modifiers: [],
            operator: :>,
            path: "timestamp",
            value: ~D[2019-01-01]
          },
          %Logflare.Lql.FilterRule{
            modifiers: [],
            path: "timestamp",
            operator: :>=,
            value: ~U[2019-01-01 03:14:15Z]
          },
          %Logflare.Lql.FilterRule{
            modifiers: [],
            operator: :"~",
            path: "event_message",
            value: "by logflare pinger"
          },
          %Logflare.Lql.FilterRule{
            modifiers: [],
            operator: :"~",
            path: "event_message",
            value: "log"
          },
          %Logflare.Lql.FilterRule{
            modifiers: [],
            operator: :"~",
            path: "event_message",
            value: "was generated"
          },
          %Logflare.Lql.FilterRule{
            modifiers: [],
            operator: :"~",
            path: "metadata.context.address",
            value: "\\d\\d\\d ST"
          },
          %Logflare.Lql.FilterRule{
            modifiers: [],
            operator: :<=,
            path: "metadata.user.cluster_id",
            value: 300
          },
          %Logflare.Lql.FilterRule{
            modifiers: [],
            operator: :>=,
            path: "metadata.user.cluster_id",
            value: 200
          },
          %Logflare.Lql.FilterRule{
            modifiers: [:negate],
            operator: :<,
            path: "metadata.log.metric4",
            value: 10
          },
          %Logflare.Lql.FilterRule{
            modifiers: [:negate],
            operator: :<=,
            path: "timestamp",
            value: ~D[2010-04-20]
          },
          %Logflare.Lql.FilterRule{
            modifiers: [:negate],
            operator: :"~",
            path: "event_message",
            value: "error"
          }
        ]
        |> Enum.sort()

      result
      |> Utils.get_filter_rules()
      |> Enum.with_index()
      |> Enum.each(fn {pathvalop, i} ->
        assert pathvalop == Enum.at(expected, i)
      end)

      assert length(Utils.get_filter_rules(result)) == length(expected)
      assert Utils.get_filter_rules(result) == expected
    end

    test "timestamp shorthands" do
      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :=,
                  path: "timestamp",
                  value: now_ndt
                }
              ]} == Parser.parse("timestamp:now", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value:
                    Timex.today()
                    |> Timex.shift(days: 1)
                    |> Timex.to_datetime()
                    |> Timex.shift(seconds: -1)
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :>=,
                  path: "timestamp",
                  value: Timex.today() |> Timex.to_datetime()
                }
              ]
              |> Enum.sort()} == Parser.parse("timestamp:today", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :>=,
                  path: "timestamp",
                  value: Timex.today() |> Timex.shift(days: -1) |> Timex.to_datetime()
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value: Timex.today() |> Timex.to_datetime() |> Timex.shift(seconds: -1)
                }
              ]
              |> Enum.sort()} == Parser.parse("timestamp:yesterday", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value: now_udt_zero_sec()
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :>=,
                  path: "timestamp",
                  value: now_udt_zero_sec()
                }
              ]} == Parser.parse("timestamp:this@minute", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value: now_udt_zero_sec()
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :>=,
                  path: "timestamp",
                  value: %{now_udt_zero_sec() | minute: 0}
                }
              ]} == Parser.parse("timestamp:this@hour", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value: now_udt_zero_sec()
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :>=,
                  path: "timestamp",
                  value: %{now_udt_zero_sec() | minute: 0, hour: 0}
                }
              ]} == Parser.parse("timestamp:this@day", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value: now_udt_zero_sec()
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :>=,
                  path: "timestamp",
                  value: Timex.beginning_of_week(%{now_udt_zero_sec() | minute: 0, hour: 0})
                }
              ]} == Parser.parse("timestamp:this@week", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value: now_udt_zero_sec()
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :>=,
                  path: "timestamp",
                  value: Timex.beginning_of_month(%{now_udt_zero_sec() | minute: 0, hour: 0})
                }
              ]} == Parser.parse("timestamp:this@month", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value: now_udt_zero_sec()
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :>=,
                  path: "timestamp",
                  value: Timex.beginning_of_year(%{now_udt_zero_sec() | minute: 0, hour: 0})
                }
              ]} == Parser.parse("timestamp:this@year", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value: now_ndt
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :>=,
                  path: "timestamp",
                  value: Timex.shift(now_ndt, seconds: -50)
                }
              ]} == Parser.parse("timestamp:last@50s", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value: now_udt_zero_sec()
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :>=,
                  path: "timestamp",
                  value: Timex.shift(now_udt_zero_sec(), minutes: -43)
                }
              ]} == Parser.parse("timestamp:last@43m", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value: now_udt_zero_sec()
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :>=,
                  path: "timestamp",
                  value: Timex.shift(%{now_udt_zero_sec() | minute: 0}, hours: -100)
                }
              ]} == Parser.parse("timestamp:last@100h", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value: now_udt_zero_sec()
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :>=,
                  path: "timestamp",
                  value: Timex.shift(%{now_udt_zero_sec() | minute: 0, hour: 0}, days: -7)
                }
              ]} == Parser.parse("timestamp:last@7d", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value: now_udt_zero_sec()
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :>=,
                  path: "timestamp",
                  value: Timex.shift(%{now_udt_zero_sec() | minute: 0, hour: 0}, weeks: -2)
                }
              ]} == Parser.parse("timestamp:last@2w", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value: now_udt_zero_sec()
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :>=,
                  path: "timestamp",
                  value:
                    Timex.shift(%{now_udt_zero_sec() | minute: 0, hour: 0, day: 1}, months: -1)
                }
              ]} == Parser.parse("timestamp:last@1mm", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value: now_udt_zero_sec()
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :>=,
                  path: "timestamp",
                  value: Timex.shift(%{now_udt_zero_sec() | minute: 0, hour: 0}, years: -1)
                }
              ]} == Parser.parse("timestamp:last@1y", @schema)
    end

    test "m,t shorthands" do
      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :=,
                  path: "metadata.user.cluster_id",
                  value: 1
                }
              ]} == Parser.parse("m.user.cluster_id:1", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :=,
                  path: "timestamp",
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
                 modifiers: [],
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
                 modifiers: [],
                 operator: :=,
                 path: "metadata.level",
                 value: "error"
               },
               %Logflare.Lql.FilterRule{
                 modifiers: [],
                 operator: :=,
                 path: "metadata.level",
                 value: "info"
               },
               %Logflare.Lql.FilterRule{
                 modifiers: [],
                 operator: :=,
                 path: "metadata.level",
                 value: "warning"
               }
             ]

      str = ~S|
         metadata.level:debug..warning
       |

      {:ok, result} = Parser.parse(str, @schema)

      assert result == [
               %Logflare.Lql.FilterRule{
                 modifiers: [],
                 operator: :=,
                 path: "metadata.level",
                 value: "debug"
               },
               %Logflare.Lql.FilterRule{
                 modifiers: [],
                 operator: :=,
                 path: "metadata.level",
                 value: "info"
               },
               %Logflare.Lql.FilterRule{
                 modifiers: [],
                 operator: :=,
                 path: "metadata.level",
                 value: "warning"
               }
             ]

      str = ~S|
         metadata.level:debug..error
       |

      {:ok, result} = Parser.parse(str, @schema)

      assert result == [
               %Logflare.Lql.FilterRule{
                 modifiers: [],
                 operator: :=,
                 path: "metadata.level",
                 value: "debug"
               },
               %Logflare.Lql.FilterRule{
                 modifiers: [],
                 operator: :=,
                 path: "metadata.level",
                 value: "error"
               },
               %Logflare.Lql.FilterRule{
                 modifiers: [],
                 operator: :=,
                 path: "metadata.level",
                 value: "info"
               },
               %Logflare.Lql.FilterRule{
                 modifiers: [],
                 operator: :=,
                 path: "metadata.level",
                 value: "warning"
               }
             ]
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
         metadata.user.cluster_group:>=200.0420..300.1337
       |

      {:ok, result} = Parser.parse(str, schema)

      assert Utils.get_filter_rules(result) == [
               %FilterRule{
                 modifiers: [],
                 operator: :<,
                 path: "metadata.log.metric5",
                 value: 10.0
               },
               %FilterRule{
                 modifiers: [],
                 operator: :<=,
                 path: "metadata.user.cluster_group",
                 value: 300.1337
               },
               %FilterRule{
                 modifiers: [],
                 operator: :>=,
                 path: "metadata.user.cluster_group",
                 value: 200.042
               }
             ]
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
              "Error while parsing timestamp filter value: expected ISO8601 string or range, got 20"} ==
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

  def now_ndt() do
    %{Timex.now() | microsecond: {0, 0}}
  end

  def now_udt_zero_sec() do
    %{now_ndt | second: 0}
  end
end
