defmodule Logflare.BigQuery.EctoQueryBQTest do
  @moduledoc false
  alias Logflare.Sources
  alias Logflare.EctoQueryBQ
  use Logflare.DataCase
  import Ecto.Query
  import Logflare.DummyFactory

  setup do
    u = insert(:user, email: System.get_env("LOGFLARE_TEST_USER_WITH_SET_IAM"))
    s = insert(:source, user_id: u.id)
    s = Sources.get_by(id: s.id)
    {:ok, sources: [s], users: [u]}
  end

  describe "where_nesteds" do
    test "1 level deep" do
      bq_table_id = System.get_env("LOGFLARE_DEV_BQ_TABLE_ID_FOR_TESTING")

      pathvalops = [
        %{
          path: "metadata.datacenter",
          value: "ne-1",
          operator: "="
        }
      ]

      q =
        from(bq_table_id)
        |> select([:timestamp, :metadata])
        |> EctoQueryBQ.where_nesteds(pathvalops)

      {sql, params} = EctoQueryBQ.SQL.to_sql(q)

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

      pathvalops = [
        %{
          path: "metadata.user.id",
          operator: "=",
          value: 5
        },
        %{
          path: "metadata.datacenter",
          value: "ne-1",
          operator: "="
        }
      ]

      q =
        from(bq_table_id)
        |> select([:timestamp, :metadata])
        |> EctoQueryBQ.where_nesteds(pathvalops)

      {sql, params} = EctoQueryBQ.SQL.to_sql(q)

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

      pathvalops = [
        %{
          path: "metadata.user.address.country",
          operator: "=",
          value: "AQ"
        }
      ]

      q =
        from(bq_table_id)
        |> select([:timestamp, :metadata])
        |> EctoQueryBQ.where_nesteds(pathvalops)

      {sql, params} = EctoQueryBQ.SQL.to_sql(q)

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

      pathvalops = [
        %{
          path: "metadata.user.address.country",
          operator: "=",
          value: "AQ"
        },
        %{
          path: "metadata.user.address.city",
          operator: "=",
          value: "Aboa"
        },
        %{
          path: "metadata.user.rating",
          operator: ">",
          value: 100
        },
        %{
          path: "metadata.context.pid",
          operator: "=",
          value: "<0.255.0>"
        },
        %{
          path: "metadata.user.exceptions",
          operator: ">=",
          value: 0
        },
        %{
          path: "metadata.user.variables",
          operator: "<=",
          value: 2
        },
        %{
          path: "metadata.user.name",
          operator: "~",
          value: "Neo"
        },
        %{
          path: "metadata.datacenter",
          operator: "=",
          value: "AWS"
        },
        %{
          path: "metadata.user.source_count",
          operator: "<",
          value: 10
        },
        %{
          path: "metadata.context.file",
          operator: "=",
          value: "lib/bigquery.ex"
        }
      ]

      q =
        from(bq_table_id)
        |> select([:timestamp, :metadata])
        |> EctoQueryBQ.where_nesteds(pathvalops)

      {sql, params} = EctoQueryBQ.SQL.to_sql(q)

      assert sql ==
               ~s|
               SELECT t0.timestamp, t0.metadata
               FROM `logflare-dev-238720`.1_dev.6efefdc5_e6fa_4193_864a_9e9daa6924d7 AS t0 INNER JOIN UNNEST(t0.metadata) AS f1 ON TRUE
               INNER JOIN UNNEST(t0.metadata) AS f2 ON TRUE
               INNER JOIN UNNEST(f2.context) AS f3 ON TRUE
               INNER JOIN UNNEST(t0.metadata) AS f4 ON TRUE
               INNER JOIN UNNEST(f4.user) AS f5 ON TRUE
               INNER JOIN UNNEST(t0.metadata) AS f6 ON TRUE
               INNER JOIN UNNEST(f6.user) AS f7 ON TRUE
               INNER JOIN UNNEST(f7.address) AS f8 ON TRUE
               WHERE (f1.datacenter = ?)
               AND (f3.pid = ?)
               AND (f3.file = ?)
               AND (f5.rating > ?)
               AND (f5.exceptions >= ?)
               AND (f5.variables <= ?)
               AND (REGEXP_CONTAINS(f5.name, ?))
               AND (f5.source_count < ?)
               AND (f8.country = ?)
               AND (f8.city = ?)|
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
                   value: "lib/bigquery.ex"
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
                   value: "Neo"
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
                   value: 10
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
                   type: "STRING"
                 },
                 parameterValue: %GoogleApi.BigQuery.V2.Model.QueryParameterValue{
                   arrayValues: nil,
                   structValues: nil,
                   value: "Aboa"
                 }
               }
             ]
    end
  end
end
