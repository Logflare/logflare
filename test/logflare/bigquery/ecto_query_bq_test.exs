defmodule Logflare.BigQuery.EctoQueryBQTest do
  @moduledoc false

  use Logflare.DataCase

  alias GoogleApi.BigQuery.V2.Model.QueryParameter
  alias GoogleApi.BigQuery.V2.Model.QueryParameterType
  alias GoogleApi.BigQuery.V2.Model.QueryParameterValue
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Lql
  alias Logflare.Lql.Rules.FilterRule

  @bq_table_id "some-table"

  setup do
    user = insert(:user)
    source = insert(:source, user_id: user.id)
    {:ok, source: source, user: user}
  end

  describe "apply_filter_rules for EctoQueryBQ.SQL" do
    test "operators are translated correctly" do
      operator_cases =
        for operator <- [:=, :<, :<=, :>, :>=] do
          {
            %FilterRule{
              path: "metadata.a",
              value: 123,
              operator: operator
            },
            [
              "INNER JOIN UNNEST(s0.metadata) AS f1 ON TRUE",
              "WHERE (f1.a #{Atom.to_string(operator)} ?)"
            ]
          }
        end

      for {rule, contains} <-
            [
              # nested string equality
              {
                %FilterRule{
                  path: "metadata.a.b",
                  value: "ne-1",
                  operator: :=
                },
                [
                  "INNER JOIN UNNEST(s0.metadata) AS f1 ON TRUE",
                  "INNER JOIN UNNEST(f1.a) AS f2 ON TRUE",
                  "WHERE (f2.b = ?)"
                ]
              },
              # regexp operator
              {
                %FilterRule{
                  path: "metadata.a",
                  value: "ne-1",
                  operator: :"~"
                },
                [
                  "INNER JOIN UNNEST(s0.metadata) AS f1 ON TRUE",
                  "WHERE (REGEXP_CONTAINS(f1.a, ?))"
                ]
              }
            ] ++ operator_cases do
        query =
          Ecto.Query.from(@bq_table_id, select: [:timestamp, :metadata])
          |> Lql.apply_filter_rules([rule])

        {:ok, {sql, params}} = BigQueryAdaptor.ecto_to_sql(query, [])
        assert sql =~ "SELECT s0.timestamp, s0.metadata"
        assert sql =~ "FROM #{@bq_table_id} AS s0"

        for str <- contains do
          assert sql =~ str
        end

        qp_type =
          case rule.value do
            v when is_binary(v) -> "STRING"
            v when is_float(v) -> "FLOAT"
            v when is_integer(v) -> "INTEGER"
          end

        assert params == [
                 %QueryParameter{
                   parameterType: %QueryParameterType{type: qp_type},
                   parameterValue: %QueryParameterValue{value: rule.value}
                 }
               ]
      end
    end

    test "top level and nested" do
      filter_rules = [
        %FilterRule{
          path: "metadata.nested",
          operator: :=,
          value: "value"
        },
        %FilterRule{
          path: "top",
          operator: :=,
          value: "level"
        }
      ]

      query =
        from(@bq_table_id)
        |> select([:timestamp, :metadata])
        |> Lql.apply_filter_rules(filter_rules)

      {:ok, {sql, _params}} = BigQueryAdaptor.ecto_to_sql(query, [])
      assert sql =~ "0.top = ?"
      assert sql =~ "1.nested = ?"
    end

    test "deeply nested" do
      filter_rules = [
        %FilterRule{
          path: "metadata.a.b.c.d.e.f.g",
          operator: :=,
          value: 5
        }
      ]

      query =
        from(@bq_table_id)
        |> select([:timestamp, :metadata])
        |> Lql.apply_filter_rules(filter_rules)

      {:ok, {sql, _params}} = BigQueryAdaptor.ecto_to_sql(query, [])

      counts = (sql |> String.split("INNER JOIN UNNEST") |> length()) - 1
      assert counts > 5
    end

    test "AND conditions" do
      filter_rules = [
        %FilterRule{
          path: "metadata.a",
          operator: :=,
          value: 123
        },
        %FilterRule{
          path: "metadata.a",
          operator: :=,
          value: 456
        }
      ]

      query =
        from(@bq_table_id)
        |> select([:timestamp, :metadata])
        |> Lql.apply_filter_rules(filter_rules)

      {:ok, {sql, params}} = BigQueryAdaptor.ecto_to_sql(query, [])
      assert sql =~ "AND"
      assert length(params) == 2
    end

    test "range operator" do
      filter_rule = %FilterRule{
        path: "metadata.latency",
        operator: :range,
        values: [100, 500]
      }

      query =
        from(@bq_table_id)
        |> select([:timestamp, :metadata])
        |> Lql.apply_filter_rules([filter_rule])

      {:ok, {sql, params}} = BigQueryAdaptor.ecto_to_sql(query, [])
      assert sql =~ "BETWEEN ? AND ?"
      assert length(params) == 2
    end

    test "NULL value operator" do
      filter_rule = %FilterRule{
        path: "metadata.optional_field",
        operator: :=,
        value: :NULL
      }

      query =
        from(@bq_table_id)
        |> select([:timestamp, :metadata])
        |> Lql.apply_filter_rules([filter_rule])

      {:ok, {sql, params}} = BigQueryAdaptor.ecto_to_sql(query, [])
      assert sql =~ "IS NULL"
      assert params == []
    end

    test "string_contains operator" do
      filter_rule = %FilterRule{
        path: "event_message",
        operator: :string_contains,
        value: "error"
      }

      query =
        from(@bq_table_id)
        |> select([:timestamp, :metadata])
        |> Lql.apply_filter_rules([filter_rule])

      {:ok, {sql, params}} = BigQueryAdaptor.ecto_to_sql(query, [])
      assert sql =~ "STRPOS(s0.event_message, ?) > 0"
      assert length(params) == 1
    end

    test "list_includes operator" do
      filter_rule = %FilterRule{
        path: "metadata.tags",
        operator: :list_includes,
        value: "production"
      }

      query =
        from(@bq_table_id)
        |> select([:timestamp, :metadata])
        |> Lql.apply_filter_rules([filter_rule])

      {:ok, {sql, params}} = BigQueryAdaptor.ecto_to_sql(query, [])
      assert sql =~ "IN UNNEST(f1.tags)"
      assert length(params) == 1
    end

    test "list_includes_regexp operator" do
      filter_rule = %FilterRule{
        path: "metadata.tags",
        operator: :list_includes_regexp,
        value: "prod.*"
      }

      query =
        from(@bq_table_id)
        |> select([:timestamp, :metadata])
        |> Lql.apply_filter_rules([filter_rule])

      {:ok, {sql, params}} = BigQueryAdaptor.ecto_to_sql(query, [])
      assert sql =~ "EXISTS(SELECT * FROM UNNEST(f1.tags) AS x WHERE REGEXP_CONTAINS(x, ?))"
      assert length(params) == 1
    end

    test "negated filters" do
      filter_rule = %FilterRule{
        path: "metadata.status",
        operator: :=,
        value: "success",
        modifiers: %{negate: true}
      }

      query =
        from(@bq_table_id)
        |> select([:timestamp, :metadata])
        |> Lql.apply_filter_rules([filter_rule])

      {:ok, {sql, params}} = BigQueryAdaptor.ecto_to_sql(query, [])
      assert sql =~ "NOT ("
      assert length(params) == 1
    end

    test "special top level fields" do
      filter_rules = [
        %FilterRule{
          path: "event_message",
          operator: :string_contains,
          value: "error"
        },
        %FilterRule{
          path: "timestamp",
          operator: :>,
          value: ~N[2023-01-01 00:00:00]
        },
        %FilterRule{
          path: "id",
          operator: :=,
          value: "12345"
        }
      ]

      query =
        from(@bq_table_id)
        |> select([:timestamp, :metadata])
        |> Lql.apply_filter_rules(filter_rules)

      {:ok, {sql, params}} = BigQueryAdaptor.ecto_to_sql(query, [])

      # Should not have UNNEST for top-level fields
      refute sql =~ "UNNEST(s0.event_message)"
      refute sql =~ "UNNEST(s0.timestamp)"
      refute sql =~ "UNNEST(s0.id)"

      # Should have direct field access
      assert sql =~ "s0.event_message"
      assert sql =~ "s0.timestamp"
      assert sql =~ "s0.id"

      assert length(params) == 3
    end

    test "mixed top level and nested filters" do
      filter_rules = [
        %FilterRule{
          path: "event_message",
          operator: :string_contains,
          value: "error"
        },
        %FilterRule{
          path: "metadata.user.id",
          operator: :=,
          value: 123
        },
        %FilterRule{
          path: "timestamp",
          operator: :>,
          value: ~N[2023-01-01 00:00:00]
        }
      ]

      query =
        from(@bq_table_id)
        |> select([:timestamp, :metadata])
        |> Lql.apply_filter_rules(filter_rules)

      {:ok, {sql, params}} = BigQueryAdaptor.ecto_to_sql(query, [])

      # Should have UNNEST for nested fields
      assert sql =~ "UNNEST(s0.metadata)"
      assert sql =~ "UNNEST(f1.user)"

      # Should not have UNNEST for top-level fields
      refute sql =~ "UNNEST(s0.event_message)"
      refute sql =~ "UNNEST(s0.timestamp)"

      assert length(params) == 3
    end

    test "complex nested path with multiple levels" do
      filter_rule = %FilterRule{
        path: "metadata.request.headers.authorization",
        operator: :=,
        value: "Bearer token"
      }

      query =
        from(@bq_table_id)
        |> select([:timestamp, :metadata])
        |> Lql.apply_filter_rules([filter_rule])

      {:ok, {sql, params}} = BigQueryAdaptor.ecto_to_sql(query, [])

      # Should have multiple UNNESTs
      assert sql =~ "UNNEST(s0.metadata) AS f1"
      assert sql =~ "UNNEST(f1.request) AS f2"
      assert sql =~ "UNNEST(f2.headers) AS f3"
      assert sql =~ "f3.authorization = ?"

      assert length(params) == 1
    end

    test "multiple filters on same nested path" do
      filter_rules = [
        %FilterRule{
          path: "metadata.user.id",
          operator: :>,
          value: 100
        },
        %FilterRule{
          path: "metadata.user.name",
          operator: :=,
          value: "admin"
        }
      ]

      query =
        from(@bq_table_id)
        |> select([:timestamp, :metadata])
        |> Lql.apply_filter_rules(filter_rules)

      {:ok, {sql, params}} = BigQueryAdaptor.ecto_to_sql(query, [])

      # Should have UNNESTs for nested paths
      unnest_count = (sql |> String.split("UNNEST") |> length()) - 1
      # at least metadata and user
      assert unnest_count >= 2

      assert length(params) == 2
    end

    test "type casting for different parameter types" do
      filter_rules = [
        %FilterRule{
          path: "metadata.string_field",
          operator: :=,
          value: "test"
        },
        %FilterRule{
          path: "metadata.int_field",
          operator: :=,
          value: 42
        },
        %FilterRule{
          path: "metadata.float_field",
          operator: :=,
          value: 3.14
        },
        %FilterRule{
          path: "metadata.bool_field",
          operator: :=,
          value: true
        }
      ]

      query =
        from(@bq_table_id)
        |> select([:timestamp, :metadata])
        |> Lql.apply_filter_rules(filter_rules)

      {:ok, {_sql, params}} = BigQueryAdaptor.ecto_to_sql(query, [])

      assert length(params) == 4

      # Check parameter types
      param_types = Enum.map(params, & &1.parameterType.type)
      assert "STRING" in param_types
      assert "INTEGER" in param_types
      assert "FLOAT" in param_types
      assert "BOOL" in param_types
    end

    test "negated operators include NULL values" do
      for operator <- [:=, :<, :<=, :>, :>=] do
        filter_rules = [
          %FilterRule{
            path: "metadata.a",
            operator: operator,
            value: 123,
            modifiers: %{negate: true}
          }
        ]

        query =
          from(@bq_table_id)
          |> select([:timestamp, :metadata])
          |> Lql.apply_filter_rules(filter_rules)

        {:ok, {sql, _params}} = BigQueryAdaptor.ecto_to_sql(query, [])

        assert sql =~ "WHERE ((f1.a IS NULL) OR NOT (f1.a #{operator} ?))"
      end
    end

    test "negated IS NULL operator shouldn't have OR IS NULL" do
      filter_rules = [
        %FilterRule{
          path: "metadata.a",
          operator: :=,
          value: :NULL,
          modifiers: %{negate: true}
        }
      ]

      query =
        from(@bq_table_id)
        |> select([:timestamp, :metadata])
        |> Lql.apply_filter_rules(filter_rules)

      {:ok, {sql, _params}} = BigQueryAdaptor.ecto_to_sql(query, [])

      assert sql =~ "WHERE (NOT (f1.a IS NULL))"
      refute sql =~ "IS NULL) OR NOT"
    end
  end

  describe "transforming PG SQL to BigQuery SQL" do
    test "subquery" do
      query =
        from(@bq_table_id)
        |> select([:id])

      {:ok, {sql, _params}} = BigQueryAdaptor.ecto_to_sql(query, [])
      assert sql == "SELECT s0.id FROM some-table AS s0", "table is unquoted"

      subquery1 = from(t in @bq_table_id, select: %{id: t.id, name: t.name})
      subquery2 = from(s in subquery(subquery1), select: %{count: fragment("COUNT(*)")})

      query =
        from(main in @bq_table_id,
          join: sub in subquery(subquery2),
          on: true,
          select: [main.id, sub.count]
        )

      {:ok, {sql, _params}} = BigQueryAdaptor.ecto_to_sql(query, [])

      assert sql ==
               "SELECT s0.id, s1.count FROM some-table AS s0 INNER JOIN (SELECT COUNT(*) AS count FROM (SELECT sss0.id AS id, sss0.name AS name FROM some-table AS sss0) AS ss0) AS s1 ON TRUE"
    end
  end
end
