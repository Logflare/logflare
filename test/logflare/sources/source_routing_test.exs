defmodule Logflare.Logs.SourceRoutingTest do
  @moduledoc false
  use Logflare.DataCase
  use Placebo
  import Logflare.Factory
  alias Logflare.{Rule, Rules}
  alias Logflare.{Users}
  alias Logflare.Source
  alias Logflare.LogEvent, as: LE
  alias Logflare.Sources
  alias Logflare.Logs.SourceRouting
  alias Logflare.Source.BigQuery.SchemaBuilder
  alias Logflare.Google.BigQuery
  alias Logflare.Lql.FilterRule, as: FR

  describe "Source Routing LQL operator rules" do
    test "list_includes operator" do
      source = build(:source, token: Faker.UUID.v4(), rules: [])

      build_filter = fn value ->
        %Rule{
          lql_string: "",
          lql_filters: [
            %FR{
              value: value,
              operator: :list_includes,
              modifiers: %{},
              path: "metadata.list_of_ints"
            }
          ]
        }
      end

      build_le = fn value ->
        build(:log_event,
          source: source,
          metadata: %{"list_of_ints" => value}
        )
      end

      le = build_le.([1, 2, 5, 0, -100, 1_000_000])
      rule = build_filter.(2)

      assert SourceRouting.route_with_lql_rules?(le, rule)

      le = build_le.([])
      rule = build_filter.(2)

      refute SourceRouting.route_with_lql_rules?(le, rule)

      le = build_le.(["2", "6", "0"])
      rule = build_filter.(nil)

      refute SourceRouting.route_with_lql_rules?(le, rule)
    end

    test "regex match operator" do
      source = build(:source, token: Faker.UUID.v4(), rules: [])

      build_filter = fn value ->
        %Rule{
          lql_string: "",
          lql_filters: [
            %FR{
              value: value,
              operator: :"~",
              modifiers: %{},
              path: "metadata.regex_string"
            }
          ]
        }
      end

      build_le = fn value ->
        build(:log_event,
          source: source,
          metadata: %{"regex_string" => value}
        )
      end

      le = build_le.("111")
      rule = build_filter.(~S|\d\d\d|)

      assert SourceRouting.route_with_lql_rules?(le, rule)

      le = build_le.("11z")
      rule = build_filter.(~S|\d\d\d|)

      refute SourceRouting.route_with_lql_rules?(le, rule)
    end

    test "gt,lt,gte,lte operators" do
      source = build(:source, token: Faker.UUID.v4(), rules: [])

      build_filter = fn value, operator ->
        %Rule{
          lql_string: "",
          lql_filters: [
            %FR{
              value: value,
              operator: operator,
              modifiers: %{},
              path: "metadata.number"
            }
          ]
        }
      end

      build_le = fn value ->
        build(:log_event,
          source: source,
          metadata: %{"number" => value}
        )
      end

      le = build_le.(100)
      rule = build_filter.(1, :>)
      assert SourceRouting.route_with_lql_rules?(le, rule)

      le = build_le.(100)
      rule = build_filter.(200, :<)
      assert SourceRouting.route_with_lql_rules?(le, rule)

      le = build_le.(1)
      rule = build_filter.(1, :>=)
      assert SourceRouting.route_with_lql_rules?(le, rule)

      le = build_le.(1)
      rule = build_filter.(1, :<=)
      assert SourceRouting.route_with_lql_rules?(le, rule)
    end

    test "multiple filters" do
      source = build(:source, token: Faker.UUID.v4(), rules: [])

      rule = %Rule{
        lql_string: "",
        lql_filters: [
          %FR{
            value: 0,
            operator: :=,
            modifiers: %{},
            path: "metadata.field1"
          },
          %FR{
            value: "string",
            operator: :"~",
            modifiers: %{},
            path: "metadata.field2"
          }
        ]
      }

      le =
        build(:log_event,
          source: source,
          metadata: %{"field1" => 0, "field2" => "string"}
        )

      assert SourceRouting.route_with_lql_rules?(le, rule)

      le =
        build(:log_event,
          source: source,
          metadata: %{"field1" => 1, "field2" => "string"}
        )

      refute SourceRouting.route_with_lql_rules?(le, rule)
    end

    test "multiple filters with negation" do
      source = build(:source, token: Faker.UUID.v4(), rules: [])

      rule = %Rule{
        lql_string: "",
        lql_filters: [
          %FR{
            value: 0,
            operator: :=,
            modifiers: %{negate: true},
            path: "metadata.field1"
          },
          %FR{
            value: "string",
            operator: :"~",
            modifiers: %{},
            path: "metadata.field2"
          }
        ]
      }

      le =
        build(:log_event,
          source: source,
          metadata: %{"field1" => 0, "field2" => "string"}
        )

      refute SourceRouting.route_with_lql_rules?(le, rule)

      le =
        build(:log_event,
          source: source,
          metadata: %{"field1" => 1, "field2" => "string"}
        )

      assert SourceRouting.route_with_lql_rules?(le, rule)
    end

    test "nested lists with maps lvl4" do
      metadata = %{
        "lines" => [
          %{
            "data" => %{
              "field1" => [
                %{
                  "field2" => [
                    %{
                      "field3" => "value"
                    }
                  ]
                }
              ]
            }
          },
          %{
            "data" => %{
              "field1" => [
                %{
                  "field2" => [
                    %{
                      "field3" => "other"
                    }
                  ]
                }
              ]
            }
          }
        ]
      }

      source = build(:source, token: Faker.UUID.v4(), rules: [])

      rule = %Rule{
        lql_string: "",
        lql_filters: [
          %FR{
            value: "value",
            operator: :=,
            modifiers: %{},
            path: ~s|metadata.lines.data.field1.field2.field3|
          }
        ]
      }

      le =
        build(:log_event,
          source: source,
          metadata: metadata
        )

      assert SourceRouting.route_with_lql_rules?(le, rule)
    end

    test "nested lists with maps lvl1" do
      source = build(:source, token: Faker.UUID.v4(), rules: [])

      metadata = %{
        "lines" => [
          %{"data" => "other_value"},
          %{"data" => "value"}
        ]
      }

      le =
        build(:log_event,
          source: source,
          metadata: metadata
        )

      rule = %Rule{
        lql_string: "",
        lql_filters: [
          %FR{
            value: "value",
            operator: :=,
            modifiers: %{},
            path: ~s|metadata.lines.data|
          }
        ]
      }

      assert SourceRouting.route_with_lql_rules?(le, rule)
    end

    test "range operator" do
      source = build(:source, token: Faker.UUID.v4(), rules: [])

      build_filter = fn lvalue, rvalue, modifiers  ->
        %Rule{
          lql_string: "",
          lql_filters: [
            %FR{
              operator: :range,
              values: [lvalue, rvalue],
              modifiers: modifiers,
              path: "metadata.range"
            }
          ]
        }
      end

      build_le = fn value ->
        build(:log_event,
          source: source,
          metadata: %{"range" => value}
        )
      end

      le = build_le.(6)
      rule = build_filter.(1, 10, %{})
      assert SourceRouting.route_with_lql_rules?(le, rule)

      rule = build_filter.(1, 10, %{negate: true})
      refute SourceRouting.route_with_lql_rules?(le, rule)

      le = build_le.(100)
      rule = build_filter.(101, 500, %{})

      refute SourceRouting.route_with_lql_rules?(le, rule)

      rule = build_filter.(101, 500, %{negate: true})
      assert SourceRouting.route_with_lql_rules?(le, rule)
    end
  end

  describe "Source Routing LQL with nested lists" do
    test "list_includes operator" do
      source = build(:source, token: Faker.UUID.v4(), rules: [])

      build_filter = fn value ->
        %Rule{
          lql_string: "",
          lql_filters: [
            %FR{
              value: value,
              operator: :list_includes,
              modifiers: %{},
              path: "metadata.level1.level2.list_of_ints"
            }
          ]
        }
      end

      le =
        build(:log_event,
          source: source,
          metadata: %{
            "level1" => [
              %{
                "level2" => [%{"list_of_ints" => [400, 300]}, %{"list_of_ints" => [100, 200, 2]}]
              },
              %{
                "level2" => [
                  %{"list_of_ints" => [2]},
                  %{"list_of_ints" => []},
                  %{"list_of_ints" => [4]}
                ]
              }
            ]
          }
        )

      rule = build_filter.(2)
      assert SourceRouting.route_with_lql_rules?(le, rule)

      rule = build_filter.(4)
      assert SourceRouting.route_with_lql_rules?(le, rule)

      rule = build_filter.(400)
      assert SourceRouting.route_with_lql_rules?(le, rule)

      rule = build_filter.(350)
      refute SourceRouting.route_with_lql_rules?(le, rule)

      rule = build_filter.(nil)
      refute SourceRouting.route_with_lql_rules?(le, rule)

      rule = build_filter.("200")
      refute SourceRouting.route_with_lql_rules?(le, rule)
    end

    test "eq operator with nested maps lvl4" do
      metadata = %{
        "lines" => [
          %{
            "data" => %{
              "field1" => [
                %{
                  "field2" => [
                    %{
                      "field3" => "value"
                    }
                  ]
                }
              ]
            }
          },
          %{
            "data" => %{
              "field1" => [
                %{
                  "field2" => [
                    %{
                      "field3" => "other"
                    }
                  ]
                }
              ]
            }
          }
        ]
      }

      source = build(:source, token: Faker.UUID.v4(), rules: [])

      rule = %Rule{
        lql_string: "",
        lql_filters: [
          %FR{
            value: "value",
            operator: :=,
            modifiers: %{},
            path: ~s|metadata.lines.data.field1.field2.field3|
          }
        ]
      }

      le =
        build(:log_event,
          source: source,
          metadata: metadata
        )

      assert SourceRouting.route_with_lql_rules?(le, rule)
    end

    test "eq operator with nested maps lvl1" do
      source = build(:source, token: Faker.UUID.v4(), rules: [])

      metadata = %{
        "lines" => [
          %{"data" => "other_value"},
          %{"data" => "value"}
        ]
      }

      le =
        build(:log_event,
          source: source,
          metadata: metadata
        )

      rule = %Rule{
        lql_string: "",
        lql_filters: [
          %FR{
            value: "value",
            operator: :=,
            modifiers: %{},
            path: ~s|metadata.lines.data|
          }
        ]
      }

      assert SourceRouting.route_with_lql_rules?(le, rule)
    end
  end

  describe "Source routing with regex routing" do
    test "successfull" do
      {:ok, _} = Source.Supervisor.start_link()
      u = Users.get_by_and_preload(email: System.get_env("LOGFLARE_TEST_USER_WITH_SET_IAM"))

      {:ok, s1} =
        params_for(:source, token: Faker.UUID.v4(), rules: [], user_id: u.id)
        |> Sources.create_source(u)

      {:ok, sink} =
        params_for(:source, token: Faker.UUID.v4(), rules: [], user_id: u.id)
        |> Sources.create_source(u)

      Process.sleep(1_000)

      schema =
        SchemaBuilder.build_table_schema(
          %{"request" => %{"url" => "/api/sources"}},
          SchemaBuilder.initial_table_schema()
        )

      {:ok, _} =
        BigQuery.patch_table(
          s1.token,
          schema,
          u.bigquery_dataset_id,
          u.bigquery_project_id || Application.get_env(:logflare, Logflare.Google)[:project_id]
        )

      Process.sleep(1_000)

      {:ok, rule} =
        Rules.create_rule(
          %{"lql_string" => ~S|"count: \d\d\d" m.request.url:~"sources$"|, "sink" => sink.token},
          s1
        )

      le =
        LE.make(
          %{
            "message" => "info count: 113",
            "metadata" => %{"request" => %{"url" => "/api/user/4/sources"}}
          },
          %{source: s1}
        )

      assert SourceRouting.route_with_lql_rules?(le, rule)

      le =
        LE.make(
          %{
            "message" => "info count: 113",
            "metadata" => %{"request" => %{"url" => "/api/user/4/sources$/4/5"}}
          },
          %{source: s1}
        )

      refute SourceRouting.route_with_lql_rules?(le, rule)
    end
  end
end
