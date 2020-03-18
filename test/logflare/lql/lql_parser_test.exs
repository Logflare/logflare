defmodule Logflare.LqlParserTest do
  @moduledoc false
  use Logflare.DataCase, async: true
  alias Logflare.Lql
  alias Logflare.Lql.Parser, as: Parser
  alias Logflare.Lql.{Utils, ChartRule, FilterRule}
  import Parser
  alias Logflare.Source.BigQuery.SchemaBuilder

  alias GoogleApi.BigQuery.V2.Model.TableFieldSchema, as: TFS
  @default_schema Logflare.BigQuery.TableSchema.SchemaBuilderHelpers.schemas().initial

  describe "LQL parsing for" do
    test "simple message search string" do
      schema = SchemaBuilder.build_table_schema(%{}, @default_schema)
      str = ~S|user sign up|
      {:ok, result} = Parser.parse(str, schema)

      lql_rules = [
        %FilterRule{operator: :"~", path: "event_message", value: "user", modifiers: []},
        %FilterRule{operator: :"~", path: "event_message", value: "sign", modifiers: []},
        %FilterRule{operator: :"~", path: "event_message", value: "up", modifiers: []}
      ]

      assert Utils.get_filter_rules(result) == lql_rules
      assert Lql.encode!(lql_rules) == str
    end

    test "quoted message search string" do
      schema = SchemaBuilder.build_table_schema(%{}, @default_schema)
      str = ~S|new "user sign up" server|
      {:ok, result} = Parser.parse(str, schema)

      lql_rules = [
        %FilterRule{operator: :"~", path: "event_message", value: "new", modifiers: []},
        %FilterRule{
          operator: :"~",
          path: "event_message",
          value: "user sign up",
          modifiers: [:quoted_string]
        },
        %FilterRule{
          operator: :"~",
          path: "event_message",
          value: "server",
          modifiers: []
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
                context: %{error_count3: 100.0, error_count2: 100.0, error_count4: 100.0, error_count1: 100.0, error_count: 100.0}
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
          modifiers: [:quoted_string],
          operator: :=,
          path: "metadata.user.type",
          value: "string"
        },
        %FilterRule{
          modifiers: [:quoted_string],
          operator: :=,
          path: "metadata.user.type",
          value: "string string"
        },
        %FilterRule{
          modifiers: [:quoted_string],
          operator: :"~",
          path: "metadata.user.type",
          value: "string"
        },
        %FilterRule{
          modifiers: [:quoted_string],
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
          modifiers: [],
          operator: :>=,
          path: "metadata.context.error_count1",
          value: 30.1
        },
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :<=,
          path: "metadata.context.error_count1",
          value: 300.1
        },
        %Logflare.Lql.FilterRule{
          modifiers: [],
          operator: :>=,
          path: "metadata.context.error_count2",
          value: 40.1
        },
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :<=,
          path: "metadata.context.error_count2",
          value: 400.1
        },
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :>=,
          path: "metadata.context.error_count3",
          value: 20.1
        },
        %Logflare.Lql.FilterRule{
          modifiers: [],
          operator: :<=,
          path: "metadata.context.error_count3",
          value: 200.1
        },
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :>=,
          path: "metadata.context.error_count4",
          value: 0.1
        },
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :<=,
          path: "metadata.context.error_count4",
          value: 0.9
        },
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :>=,
          path: "metadata.users.source_count",
          value: 50
        },
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :<=,
          path: "metadata.users.source_count",
          value: 200
        },
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
        timestamp:2019-01-01T00:13:37Z..2019-02-01T00:23:34Z
      |

      {:ok, result} = Parser.parse(str, schema)

      lql_rules = [
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :>=,
          path: "metadata.context.error_count",
          value: 100
        },
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :"~",
          path: "metadata.user.about",
          value: "referrall"
        },
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :<,
          path: "metadata.user.id",
          value: 1
        },
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :=,
          path: "metadata.user.type",
          value: "paid"
        },
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :<=,
          path: "metadata.user.views",
          value: 1
        },
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :>,
          path: "metadata.users.source_count",
          value: 100
        },
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :>=,
          path: "timestamp",
          value: ~U[2019-01-01 00:13:37Z]
        },
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :<=,
          path: "timestamp",
          value: ~U[2019-02-01 00:23:34Z]
        }
      ]

      assert Utils.get_filter_rules(result) == lql_rules

      assert Lql.encode!(lql_rules) == clean_and_trim_lql_string(str)

      str = ~S|
        timestamp:2019-01-01..2019-02-01
      |

      lql_rules = [
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :>=,
          path: "timestamp",
          value: ~D[2019-01-01]
        },
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :<=,
          path: "timestamp",
          value: ~D[2019-02-01]
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
         chart:metadata.log.metric4
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
       |

      {:ok, lql_rules} = Parser.parse(str, schema)

      filters = [
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :"~",
          path: "event_message",
          value: "log"
        },
        %Logflare.Lql.FilterRule{
          modifiers: [:quoted_string],
          operator: :"~",
          path: "event_message",
          value: "was generated"
        },
        %Logflare.Lql.FilterRule{
          modifiers: [:quoted_string],
          operator: :"~",
          path: "event_message",
          value: "by logflare pinger"
        },
        %Logflare.Lql.FilterRule{
          modifiers: [:quoted_string],
          operator: :=,
          path: "metadata.context.file",
          value: "some module.ex"
        },
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :=,
          path: "metadata.context.line_number",
          value: 100
        },
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :"~",
          path: "metadata.log.label1",
          value: "origin"
        },
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :<,
          path: "metadata.log.metric1",
          value: 10
        },
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :<=,
          path: "metadata.log.metric2",
          value: 10
        },
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :>,
          path: "metadata.log.metric3",
          value: 10
        },
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :>=,
          path: "metadata.log.metric4",
          value: 10
        },
        %Logflare.Lql.FilterRule{modifiers: [], operator: :=, path: "metadata.user.admin", value: false},
        %Logflare.Lql.FilterRule{modifiers: [], operator: :=, path: "metadata.user.group_id", value: 5}
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
      timestamp:>2019-01-01
      timestamp:<=2019-04-20
      timestamp:<2020-01-01T03:14:15Z
      timestamp:>=2019-01-01T03:14:15Z
      timestamp:<=2010-04-20|

      {:ok, result} = Parser.parse(str, @schema)

      expected = [
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :"~",
          path: "event_message",
          value: "log"
        },
        %Logflare.Lql.FilterRule{
          modifiers: [:quoted_string],
          operator: :"~",
          path: "event_message",
          value: "was generated"
        },
        %Logflare.Lql.FilterRule{
          modifiers: [:quoted_string],
          operator: :"~",
          path: "event_message",
          value: "by logflare pinger"
        },
        %Logflare.Lql.FilterRule{modifiers: [], operator: :"~", path: "event_message", value: "error"},
        %Logflare.Lql.FilterRule{modifiers: [:quoted_string], operator: :"~", path: "metadata.context.address", value: "\\\\d\\\\d\\\\d ST"},
        %Logflare.Lql.FilterRule{modifiers: [:quoted_string], operator: :=, path: "metadata.context.file", value: "some module.ex"},
        %Logflare.Lql.FilterRule{modifiers: [], operator: :=, path: "metadata.context.line_number", value: 100},
        %Logflare.Lql.FilterRule{modifiers: [], operator: :<, path: "metadata.log.metric1", value: 10},
        %Logflare.Lql.FilterRule{modifiers: [], operator: :<, path: "metadata.log.metric4", value: 10},
        %Logflare.Lql.FilterRule{modifiers: [], operator: :>=, path: "metadata.user.cluster_id", value: 200},
        %Logflare.Lql.FilterRule{modifiers: [], operator: :<=, path: "metadata.user.cluster_id", value: 300},
        %Logflare.Lql.FilterRule{modifiers: [], operator: :=, path: "metadata.user.group_id", value: 5},
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :>,
          path: "timestamp",
          value: ~D[2019-01-01]
        },
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :<=,
          path: "timestamp",
          value: ~D[2019-04-20]
        },
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :<,
          path: "timestamp",
          value: ~U[2020-01-01 03:14:15Z]
        },
        %Logflare.Lql.FilterRule{
          modifiers: '',
          operator: :>=,
          path: "timestamp",
          value: ~U[2019-01-01 03:14:15Z]
        },
        %Logflare.Lql.FilterRule{
          modifiers: [],
          operator: :<=,
          path: "timestamp",
          value: ~D[2010-04-20]
        }
      ]

      assert Utils.get_filter_rules(result) == expected

      assert length(Utils.get_filter_rules(result)) == length(expected)

      assert Lql.encode!(result) == clean_and_trim_lql_string(str)
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
                  operator: :>=,
                  path: "timestamp",
                  value: Timex.today() |> Timex.to_datetime()
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value:
                    Timex.today()
                    |> Timex.shift(days: 1)
                    |> Timex.to_datetime()
                    |> Timex.shift(seconds: -1)
                }
              ]} == Parser.parse("timestamp:today", @schema)

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
              ]} == Parser.parse("timestamp:yesterday", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :>=,
                  path: "timestamp",
                  value: now_udt_zero_sec()
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value: now_udt_zero_sec()
                }
              ]} == Parser.parse("timestamp:this@minute", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :>=,
                  path: "timestamp",
                  value: %{now_udt_zero_sec() | minute: 0}
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value: now_udt_zero_sec()
                }
              ]} == Parser.parse("timestamp:this@hour", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :>=,
                  path: "timestamp",
                  value: %{now_udt_zero_sec() | minute: 0, hour: 0}
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value: now_udt_zero_sec()
                }
              ]} == Parser.parse("timestamp:this@day", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :>=,
                  path: "timestamp",
                  value: Timex.beginning_of_week(%{now_udt_zero_sec() | minute: 0, hour: 0})
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value: now_udt_zero_sec()
                }
              ]} == Parser.parse("timestamp:this@week", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :>=,
                  path: "timestamp",
                  value: Timex.beginning_of_month(%{now_udt_zero_sec() | minute: 0, hour: 0})
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value: now_udt_zero_sec()
                }
              ]} == Parser.parse("timestamp:this@month", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :>=,
                  path: "timestamp",
                  value: Timex.beginning_of_year(%{now_udt_zero_sec() | minute: 0, hour: 0})
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value: now_udt_zero_sec()
                }
              ]} == Parser.parse("timestamp:this@year", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :>=,
                  path: "timestamp",
                  value: Timex.shift(now_ndt, seconds: -50)
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value: now_ndt
                }
              ]} == Parser.parse("timestamp:last@50s", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :>=,
                  path: "timestamp",
                  value: Timex.shift(now_udt_zero_sec(), minutes: -43)
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value: now_udt_zero_sec()
                }
              ]} == Parser.parse("timestamp:last@43m", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :>=,
                  path: "timestamp",
                  value: Timex.shift(%{now_udt_zero_sec() | minute: 0}, hours: -100)
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value: now_udt_zero_sec()
                }
              ]} == Parser.parse("timestamp:last@100h", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :>=,
                  path: "timestamp",
                  value: Timex.shift(%{now_udt_zero_sec() | minute: 0, hour: 0}, days: -7)
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value: now_udt_zero_sec()
                }
              ]} == Parser.parse("timestamp:last@7d", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :>=,
                  path: "timestamp",
                  value: Timex.shift(%{now_udt_zero_sec() | minute: 0, hour: 0}, weeks: -2)
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value: now_udt_zero_sec()
                }
              ]} == Parser.parse("timestamp:last@2w", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :>=,
                  path: "timestamp",
                  value:
                    Timex.shift(%{now_udt_zero_sec() | minute: 0, hour: 0, day: 1}, months: -1)
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value: now_udt_zero_sec()
                }
              ]} == Parser.parse("timestamp:last@1mm", @schema)

      assert {:ok,
              [
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :>=,
                  path: "timestamp",
                  value: Timex.shift(%{now_udt_zero_sec() | minute: 0, hour: 0}, years: -1)
                },
                %Logflare.Lql.FilterRule{
                  modifiers: [],
                  operator: :<=,
                  path: "timestamp",
                  value: now_udt_zero_sec()
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
                 value: "info"
               },
               %Logflare.Lql.FilterRule{
                 modifiers: '',
                 operator: :=,
                 path: "metadata.level",
                 value: "warning"
               },
               %Logflare.Lql.FilterRule{
                 modifiers: '',
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

      assert Lql.encode!(lql_rules) == clean_and_trim_lql_string(str)

      str = ~S|
         metadata.level:debug..error
       |

      {:ok, lql_rules} = Parser.parse(str, @schema)

      assert lql_rules == [
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
               },
               %Logflare.Lql.FilterRule{
                 modifiers: [],
                 operator: :=,
                 path: "metadata.level",
                 value: "error"
               }
             ]

      assert Lql.encode!(lql_rules) == clean_and_trim_lql_string(str)
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
          modifiers: [],
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
          modifiers: [:quoted_string],
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
          modifiers: [],
          operator: :list_includes,
          path: "metadata.integer_array",
          value: 1
        }
      ]

      assert {:ok, filter} == Parser.parse("m.integer_array:@>1", @schema)

      filter = [
        %Logflare.Lql.FilterRule{
          modifiers: [:quoted_string],
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
          modifiers: [],
          operator: :list_includes,
          path: "metadata.float_array",
          value: 1.0
        }
      ]

      assert {:ok, filter} == Parser.parse("m.float_array:@>1.0", @schema)

      filter = [
        %Logflare.Lql.FilterRule{
          modifiers: [:quoted_string],
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
               %FilterRule{
                 modifiers: [],
                 operator: :<,
                 path: "metadata.log.metric5",
                 value: 10.0
               },
               %FilterRule{
                 modifiers: [],
                 operator: :>=,
                 path: "metadata.user.cluster_group",
                 value: 200.042
               },
               %FilterRule{
                 modifiers: [],
                 operator: :<=,
                 path: "metadata.user.cluster_group",
                 value: 300.1337
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
         c:m.log.metric5
         c:period@minute
         c:aggregate@sum
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
         chart:metadata.log.metric5
         chart:aggregate@sum
         chart:period@minute
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

      assert Lql.encode!(result) == clean_and_trim_lql_string(str)
    end

      str = ~S|
         chart:m.log.metric5
         chart:period@minute
         chart:aggregate@sum
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

  def clean_and_trim_lql_string(str) do
    str |> String.replace(~r/\s{2,}/, " ") |> String.trim()
  end
end
