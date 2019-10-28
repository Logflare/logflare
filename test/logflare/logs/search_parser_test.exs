defmodule Logflare.Logs.Search.ParserTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Logs.Search.Parser
  alias Logflare.Source.BigQuery.SchemaBuilder

  alias GoogleApi.BigQuery.V2.Model.TableFieldSchema, as: TFS
  @default_schema Logflare.BigQuery.TableSchema.SchemaBuilderHelpers.schemas().initial

  describe "search query parser for" do
    test "simple message search string" do
      schema = SchemaBuilder.build_table_schema(%{}, @default_schema)
      str = ~S|user sign up|
      {:ok, result} = Parser.parse(str, schema)

      assert result == [
               %{operator: "~", path: "event_message", value: "user"},
               %{operator: "~", path: "event_message", value: "sign"},
               %{operator: "~", path: "event_message", value: "up"}
             ]
    end

    test "quoted message search string" do
      schema = SchemaBuilder.build_table_schema(%{}, @default_schema)
      str = ~S|new "user sign up" server|
      {:ok, result} = Parser.parse(str, schema)

      assert Enum.sort(result) ==
               Enum.sort([
                 %{operator: "~", path: "event_message", value: "user sign up"},
                 %{operator: "~", path: "event_message", value: "new"},
                 %{operator: "~", path: "event_message", value: "server"}
               ])
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
        timestamp:2019-01-01T00:13:37..2019-02-01T00:23:34
      |

      {:ok, result} = Parser.parse(str, schema)

      assert result == [
               %{operator: "=", path: "metadata.user.type", value: "paid"},
               %{operator: "<", path: "metadata.user.id", value: 1},
               %{operator: "<=", path: "metadata.user.views", value: 1},
               %{operator: ">", path: "metadata.users.source_count", value: 100},
               %{operator: ">=", path: "metadata.context.error_count", value: 100},
               %{operator: "~", path: "metadata.user.about", value: "referrall"},
               %{operator: ">=", path: "timestamp", value: "2019-01-01"},
               %{operator: "<=", path: "timestamp", value: "2019-02-01"},
               %{operator: ">=", path: "timestamp", value: "2019-01-01T00:13:37"},
               %{operator: "<=", path: "timestamp", value: "2019-02-01T00:23:34"}
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
       |

      {:ok, result} = Parser.parse(str, schema)

      assert Enum.sort(result) ==
               Enum.sort([
                 %{operator: "<", path: "metadata.log.metric1", value: 10},
                 %{operator: "<=", path: "metadata.log.metric2", value: 10},
                 %{operator: "=", path: "metadata.context.file", value: "some module.ex"},
                 %{operator: "=", path: "metadata.context.line_number", value: 100},
                 %{operator: "=", path: "metadata.user.admin", value: false},
                 %{operator: "=", path: "metadata.user.group_id", value: 5},
                 %{value: 10, operator: ">", path: "metadata.log.metric3"},
                 %{operator: ">=", path: "metadata.log.metric4", value: 10},
                 %{operator: "~", path: "event_message", value: "by logflare pinger"},
                 %{operator: "~", path: "event_message", value: "log"},
                 %{operator: "~", path: "event_message", value: "was generated"},
                 %{operator: "~", path: "metadata.log.label1", value: "origin"}
               ])
    end

    @tag :run
    test "nested fields filter with timestamp 3" do
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
         -metadata.log.metric1:<10
         -timestamp:<=2010-04-20
         -error
       |

      {:ok, result} = Parser.parse(str, schema)
      sorted_result = Enum.sort(result)

      expected =
        Enum.sort([
          %{operator: "!<", path: "metadata.log.metric1", value: 10},
          %{operator: "!<=", path: "timestamp", value: ~D[2010-04-20]},
          %{operator: "!~", path: "event_message", value: "error"},
          %{operator: "<", path: "metadata.log.metric1", value: 10},
          %{operator: "<", path: "timestamp", value: ~U[2020-01-01 03:14:15Z]},
          %{operator: "<=", path: "timestamp", value: ~D[2019-04-20]},
          %{operator: "=", path: "metadata.context.file", value: "some module.ex"},
          %{operator: "=", path: "metadata.context.line_number", value: 100},
          %{operator: "=", path: "metadata.user.cluster_id", value: "200..300"},
          %{operator: "=", path: "metadata.user.group_id", value: 5},
          %{operator: ">", path: "timestamp", value: ~D[2019-01-01]},
          %{operator: ">=", path: "timestamp", value: ~U[2019-01-01 03:14:15Z]},
          %{operator: "~", path: "event_message", value: "by logflare pinger"},
          %{operator: "~", path: "event_message", value: "log"},
          %{operator: "~", path: "event_message", value: "was generated"},
          %{operator: "~", path: "metadata.context.address", value: "\\d\\d\\d ST"}
        ])

      assert length(sorted_result) == length(expected)

      sorted_result
      |> Enum.with_index()
      |> Enum.each(fn {pathvalop, i} ->
        assert pathvalop == Enum.at(expected, i)
      end)

      assert sorted_result == expected
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

      assert Enum.sort(result) ==
               Enum.sort([
                 %{operator: "<", path: "metadata.log.metric5", value: 10.0},
                 %{
                   path: "metadata.user.cluster_group",
                   value: "200.0420..300.1337",
                   operator: ">="
                 }
               ])
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

      assert {:error, "Timestamp parse error: invalid_format"} = Parser.parse(str, schema)
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

      assert {:error, "Invalid query! Please consult search syntax guide."} =
               Parser.parse(str, schema)
    end
  end
end
