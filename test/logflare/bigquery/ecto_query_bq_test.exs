defmodule Logflare.BigQuery.EctoQueryBQTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.{EctoQueryBQ, Lql, Lql.FilterRule}
  alias GoogleApi.BigQuery.V2.Model.{QueryParameter, QueryParameterType, QueryParameterValue}
  @bq_table_id "some-table"

  setup do
    user = insert(:user)
    source = insert(:source, user_id: user.id)
    {:ok, source: source, user: user}
  end

  describe "EctoQueryBQ.SQL and apply_filter_rules_to_query" do
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
          |> Lql.EctoHelpers.apply_filter_rules_to_query([rule])

        {sql, params} = EctoQueryBQ.SQL.to_sql_params(query)
        assert sql =~ "SELECT s0.timestamp, s0.metadata"
        assert sql =~ "FROM #{@bq_table_id} AS s0"

        for str <- contains do
          assert sql =~ str
        end

        qp_type =
          case rule.value do
            v when is_binary(v) -> "STRING"
            v when is_float(v) -> "FLOAT"
            v when is_integer(v) -> "FLOAT"
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
        |> Lql.EctoHelpers.apply_filter_rules_to_query(filter_rules)

      {sql, _params} = EctoQueryBQ.SQL.to_sql_params(query)
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
        |> Lql.EctoHelpers.apply_filter_rules_to_query(filter_rules)

      {sql, _params} = EctoQueryBQ.SQL.to_sql_params(query)

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
        |> Lql.EctoHelpers.apply_filter_rules_to_query(filter_rules)

      {sql, params} = EctoQueryBQ.SQL.to_sql_params(query)
      assert sql =~ "AND"
      assert length(params) == 2
    end
  end
end
