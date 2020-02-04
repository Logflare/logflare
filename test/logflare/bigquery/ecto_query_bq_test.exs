defmodule Logflare.BigQuery.Lql.EctoHelpersTest do
  @moduledoc false
  alias Logflare.Sources
  alias Logflare.Users
  alias Logflare.Lql
  alias Logflare.EctoQueryBQ
  use Logflare.DataCase
  alias Logflare.Lql.FilterRule
  import Ecto.Query
  import Logflare.Factory

  setup do
    u = Users.get_by(email: System.get_env("LOGFLARE_TEST_USER_WITH_SET_IAM"))
    s = insert(:source, user_id: u.id)
    s = Sources.get_by(id: s.id)
    {:ok, sources: [s], users: [u]}
  end

  describe "apply_filter_rules_to_query" do
    test "1 level deep" do
      bq_table_id = System.get_env("LOGFLARE_DEV_BQ_TABLE_ID_FOR_TESTING")

      filter_rules = [
        %FilterRule{
          path: "metadata.datacenter",
          value: "ne-1",
          operator: :=,
          modifiers: []
        }
      ]

      q =
        from(bq_table_id)
        |> select([:timestamp, :metadata])
        |> Lql.EctoHelpers.apply_filter_rules_to_query(filter_rules)

      {sql, params} = EctoQueryBQ.SQL.to_sql_params(q)

      assert sql ==
               ~s|
               SELECT t0.timestamp, t0.metadata
               FROM #{bq_table_id} AS t0 INNER JOIN UNNEST(t0.metadata) AS f1 ON TRUE
               WHERE (f1.datacenter = ?)|
               |> String.replace(~r/[\s]+/, " ")
               |> String.trim()

      assert params == [
               %GoogleApi.BigQuery.V2.Model.QueryParameter{
                 name: nil,
                 parameterType: %GoogleApi.BigQuery.V2.Model.QueryParameterType{
                   arrayType: nil,
                   structTypes: nil,
                   type: "STRING"
                 },
                 parameterValue: %GoogleApi.BigQuery.V2.Model.QueryParameterValue{
                   arrayValues: nil,
                   structValues: nil,
                   value: "ne-1"
                 }
               }
             ]
    end

    test "2 and 3 level deep" do
      bq_table_id = System.get_env("LOGFLARE_DEV_BQ_TABLE_ID_FOR_TESTING")

      filter_rules =
        [
          %FilterRule{
            path: "metadata.user.id",
            operator: :=,
            value: 5,
            modifiers: []
          },
          %FilterRule{
            path: "metadata.datacenter",
            value: "ne-1",
            operator: :=,
            modifiers: []
          }
        ]
        |> Enum.sort()

      q =
        from(bq_table_id)
        |> select([:timestamp, :metadata])
        |> Lql.EctoHelpers.apply_filter_rules_to_query(filter_rules)

      {sql, params} = EctoQueryBQ.SQL.to_sql_params(q)

      assert sql ==
               ~s|
               SELECT t0.timestamp, t0.metadata
               FROM `logflare-dev-238720`.1_dev.6efefdc5_e6fa_4193_864a_9e9daa6924d7 AS t0
               INNER JOIN UNNEST(t0.metadata) AS f1 ON TRUE
               INNER JOIN UNNEST(t0.metadata) AS f2 ON TRUE
               INNER JOIN UNNEST(f2.user) AS f3 ON TRUE
               WHERE (f1.datacenter = ?) AND (f3.id = ?)
               |
               |> String.replace(~r/[\s]+/, " ")
               |> String.trim()

      assert params == [
               %GoogleApi.BigQuery.V2.Model.QueryParameter{
                 name: nil,
                 parameterType: %GoogleApi.BigQuery.V2.Model.QueryParameterType{
                   arrayType: nil,
                   structTypes: nil,
                   type: "STRING"
                 },
                 parameterValue: %GoogleApi.BigQuery.V2.Model.QueryParameterValue{
                   arrayValues: nil,
                   structValues: nil,
                   value: "ne-1"
                 }
               },
               %GoogleApi.BigQuery.V2.Model.QueryParameter{
                 name: nil,
                 parameterType: %GoogleApi.BigQuery.V2.Model.QueryParameterType{
                   arrayType: nil,
                   structTypes: nil,
                   type: "INTEGER"
                 },
                 parameterValue: %GoogleApi.BigQuery.V2.Model.QueryParameterValue{
                   arrayValues: nil,
                   structValues: nil,
                   value: 5
                 }
               }
             ]

      filter_rules = [
        %FilterRule{
          path: "metadata.user.address.country",
          operator: :=,
          value: "AQ",
          modifiers: []
        }
      ]

      q =
        from(bq_table_id)
        |> select([:timestamp, :metadata])
        |> Lql.EctoHelpers.apply_filter_rules_to_query(filter_rules)

      {sql, params} = EctoQueryBQ.SQL.to_sql_params(q)

      assert sql ==
               ~s|
               SELECT t0.timestamp, t0.metadata
               FROM `logflare-dev-238720`.1_dev.6efefdc5_e6fa_4193_864a_9e9daa6924d7 AS t0
               INNER JOIN UNNEST(t0.metadata) AS f1 ON TRUE
               INNER JOIN UNNEST(f1.user) AS f2 ON TRUE
               INNER JOIN UNNEST(f2.address) AS f3 ON TRUE
               WHERE (f3.country = ?)
               |
               |> String.replace(~r/[\s]+/, " ")
               |> String.trim()

      assert params == [
               %GoogleApi.BigQuery.V2.Model.QueryParameter{
                 name: nil,
                 parameterType: %GoogleApi.BigQuery.V2.Model.QueryParameterType{
                   arrayType: nil,
                   structTypes: nil,
                   type: "STRING"
                 },
                 parameterValue: %GoogleApi.BigQuery.V2.Model.QueryParameterValue{
                   arrayValues: nil,
                   structValues: nil,
                   value: "AQ"
                 }
               }
             ]
    end

    test "multiple paths with various depth levels" do
      bq_table_id = System.get_env("LOGFLARE_DEV_BQ_TABLE_ID_FOR_TESTING")

      filter_rules =
        [
          %FilterRule{
            path: "metadata.user.address.country",
            operator: :=,
            value: "AQ",
            modifiers: []
          },
          %FilterRule{
            path: "metadata.user.address.city",
            operator: :=,
            value: "Aboa",
            modifiers: []
          },
          %FilterRule{
            path: "metadata.user.rating",
            operator: :>,
            value: 100,
            modifiers: []
          },
          %FilterRule{
            path: "metadata.context.pid",
            operator: :=,
            value: "<0.255.0>",
            modifiers: []
          },
          %FilterRule{
            path: "metadata.user.exceptions",
            operator: :>=,
            value: 0,
            modifiers: []
          },
          %FilterRule{
            path: "metadata.user.variables",
            operator: :<=,
            value: 2,
            modifiers: []
          },
          %FilterRule{
            path: "metadata.user.name",
            operator: :"~",
            value: "Neo",
            modifiers: []
          },
          %FilterRule{
            path: "metadata.datacenter",
            operator: :=,
            value: "AWS",
            modifiers: []
          },
          %FilterRule{
            path: "metadata.user.source_count",
            operator: :<,
            value: 10,
            modifiers: []
          },
          %FilterRule{
            path: "metadata.context.file",
            operator: :=,
            value: "lib/bigquery.ex",
            modifiers: []
          }
        ]
        |> Enum.sort()

      ensure_path_atoms_exist(filter_rules)

      q =
        from(bq_table_id)
        |> select([:timestamp, :metadata])
        |> Lql.EctoHelpers.apply_filter_rules_to_query(filter_rules)

      {sql, params} = Logflare.EctoQueryBQ.SQL.to_sql_params(q)

      sql ==
        ~s|
      SELECT t0.timestamp,
      t0.metadata
      FROM `logflare-dev-238720`.1_dev.6efefdc5_e6fa_4193_864a_9e9daa6924d7 AS t0
      INNER JOIN UNNEST(t0.metadata) AS f1 ON TRUE
      INNER JOIN UNNEST(f1.user) AS f2 ON TRUE
      INNER JOIN UNNEST(t0.metadata) AS f3 ON TRUE
      INNER JOIN UNNEST(f3.user) AS f4 ON TRUE
      INNER JOIN UNNEST(t0.metadata) AS f5 ON TRUE
      INNER JOIN UNNEST(f5.context) AS f6 ON TRUE
      INNER JOIN UNNEST(t0.metadata) AS f7 ON TRUE
      INNER JOIN UNNEST(f7.context) AS f8 ON TRUE
      INNER JOIN UNNEST(t0.metadata) AS f9 ON TRUE
      INNER JOIN UNNEST(t0.metadata) AS f10 ON TRUE
      INNER JOIN UNNEST(f10.user) AS f11 ON TRUE
      INNER JOIN UNNEST(f11.address) AS f12 ON TRUE
      INNER JOIN UNNEST(t0.metadata) AS f13 ON TRUE
      INNER JOIN UNNEST(f13.user) AS f14 ON TRUE
      INNER JOIN UNNEST(f14.address) AS f15 ON TRUE
      INNER JOIN UNNEST(t0.metadata) AS f16 ON TRUE
      INNER JOIN UNNEST(f16.user) AS f17 ON TRUE
      INNER JOIN UNNEST(t0.metadata) AS f18 ON TRUE
      INNER JOIN UNNEST(f18.user) AS f19 ON TRUE
      INNER JOIN UNNEST(t0.metadata) AS f20 ON TRUE
      INNER JOIN UNNEST(f20.user) AS f21 ON TRUE
      WHERE (f2.source_count < ?)
        AND (f4.variables <= ?)
        AND (f6.file = ?)
        AND (f8.pid = ?)
        AND (f9.datacenter = ?)
        AND (f12.city = ?)
        AND (f15.country = ?)
        AND (f17.rating > ?)
        AND (f19.exceptions >= ?)
        AND (REGEXP_CONTAINS(f21.name, ?))
      |
        |> String.replace(~r/[\s]+/, " ")
        |> String.trim()

      assert params == [
               %GoogleApi.BigQuery.V2.Model.QueryParameter{
                 name: nil,
                 parameterType: %GoogleApi.BigQuery.V2.Model.QueryParameterType{
                   arrayType: nil,
                   structTypes: nil,
                   type: "INTEGER"
                 },
                 parameterValue: %GoogleApi.BigQuery.V2.Model.QueryParameterValue{
                   arrayValues: nil,
                   structValues: nil,
                   value: 10
                 }
               },
               %GoogleApi.BigQuery.V2.Model.QueryParameter{
                 name: nil,
                 parameterType: %GoogleApi.BigQuery.V2.Model.QueryParameterType{
                   arrayType: nil,
                   structTypes: nil,
                   type: "INTEGER"
                 },
                 parameterValue: %GoogleApi.BigQuery.V2.Model.QueryParameterValue{
                   arrayValues: nil,
                   structValues: nil,
                   value: 2
                 }
               },
               %GoogleApi.BigQuery.V2.Model.QueryParameter{
                 name: nil,
                 parameterType: %GoogleApi.BigQuery.V2.Model.QueryParameterType{
                   arrayType: nil,
                   structTypes: nil,
                   type: "STRING"
                 },
                 parameterValue: %GoogleApi.BigQuery.V2.Model.QueryParameterValue{
                   arrayValues: nil,
                   structValues: nil,
                   value: "lib/bigquery.ex"
                 }
               },
               %GoogleApi.BigQuery.V2.Model.QueryParameter{
                 name: nil,
                 parameterType: %GoogleApi.BigQuery.V2.Model.QueryParameterType{
                   arrayType: nil,
                   structTypes: nil,
                   type: "STRING"
                 },
                 parameterValue: %GoogleApi.BigQuery.V2.Model.QueryParameterValue{
                   arrayValues: nil,
                   structValues: nil,
                   value: "<0.255.0>"
                 }
               },
               %GoogleApi.BigQuery.V2.Model.QueryParameter{
                 name: nil,
                 parameterType: %GoogleApi.BigQuery.V2.Model.QueryParameterType{
                   arrayType: nil,
                   structTypes: nil,
                   type: "STRING"
                 },
                 parameterValue: %GoogleApi.BigQuery.V2.Model.QueryParameterValue{
                   arrayValues: nil,
                   structValues: nil,
                   value: "AWS"
                 }
               },
               %GoogleApi.BigQuery.V2.Model.QueryParameter{
                 name: nil,
                 parameterType: %GoogleApi.BigQuery.V2.Model.QueryParameterType{
                   arrayType: nil,
                   structTypes: nil,
                   type: "STRING"
                 },
                 parameterValue: %GoogleApi.BigQuery.V2.Model.QueryParameterValue{
                   arrayValues: nil,
                   structValues: nil,
                   value: "Aboa"
                 }
               },
               %GoogleApi.BigQuery.V2.Model.QueryParameter{
                 name: nil,
                 parameterType: %GoogleApi.BigQuery.V2.Model.QueryParameterType{
                   arrayType: nil,
                   structTypes: nil,
                   type: "STRING"
                 },
                 parameterValue: %GoogleApi.BigQuery.V2.Model.QueryParameterValue{
                   arrayValues: nil,
                   structValues: nil,
                   value: "AQ"
                 }
               },
               %GoogleApi.BigQuery.V2.Model.QueryParameter{
                 name: nil,
                 parameterType: %GoogleApi.BigQuery.V2.Model.QueryParameterType{
                   arrayType: nil,
                   structTypes: nil,
                   type: "INTEGER"
                 },
                 parameterValue: %GoogleApi.BigQuery.V2.Model.QueryParameterValue{
                   arrayValues: nil,
                   structValues: nil,
                   value: 100
                 }
               },
               %GoogleApi.BigQuery.V2.Model.QueryParameter{
                 name: nil,
                 parameterType: %GoogleApi.BigQuery.V2.Model.QueryParameterType{
                   arrayType: nil,
                   structTypes: nil,
                   type: "INTEGER"
                 },
                 parameterValue: %GoogleApi.BigQuery.V2.Model.QueryParameterValue{
                   arrayValues: nil,
                   structValues: nil,
                   value: 0
                 }
               },
               %GoogleApi.BigQuery.V2.Model.QueryParameter{
                 name: nil,
                 parameterType: %GoogleApi.BigQuery.V2.Model.QueryParameterType{
                   arrayType: nil,
                   structTypes: nil,
                   type: "STRING"
                 },
                 parameterValue: %GoogleApi.BigQuery.V2.Model.QueryParameterValue{
                   arrayValues: nil,
                   structValues: nil,
                   value: "Neo"
                 }
               }
             ]
    end
  end

  def ensure_path_atoms_exist(filter_rules) do
    for %{path: path} <- filter_rules do
      path
      |> String.split(".")
      |> Enum.map(&String.to_atom/1)
    end
  end
end
