defmodule Logflare.BigQuery.EctoQueryBQ do
  @moduledoc false
  alias Logflare.Sources
  alias Logflare.EctoQueryBQ
  use Logflare.DataCase
  import Ecto.Query
  import Logflare.DummyFactory
  alias Logflare.EctoQueryBQ.NestedPath

  setup do
    u = insert(:user, email: System.get_env("LOGFLARE_TEST_USER_WITH_SET_IAM"))
    s = insert(:source, user_id: u.id)
    s = Sources.get_by(id: s.id)
    {:ok, sources: [s], users: [u]}
  end

  describe "where_nested_eq" do
    test "1 level deep", %{sources: [source | _], users: [user | _]} do
      bq_table_id = System.get_env("LOGFLARE_DEV_BQ_TABLE_ID_FOR_TESTING")

      path = "metadata.datacenter"
      value = "ne-1"

      q =
        from(bq_table_id)
        |> select([:timestamp, :metadata])
        |> EctoQueryBQ.where_nested_eq(path, value)

      {sql, params} = Ecto.Adapters.SQL.to_sql(:all, Repo, q)

      sql = EctoQueryBQ.ecto_pg_sql_to_bq_sql(sql)

      assert sql ==
               ~s|SELECT t0.timestamp, t0.metadata FROM #{bq_table_id} AS t0 INNER JOIN UNNEST(t0.metadata) AS f1 ON TRUE WHERE (f1.datacenter = ?)|

      assert params == ["ne-1"]
    end

    test "2 and 3 level deep", %{sources: [source | _], users: [user | _]} do
      bq_table_id = System.get_env("LOGFLARE_DEV_BQ_TABLE_ID_FOR_TESTING")

      path = "metadata.user.id"
      value = 5

      q =
        from(bq_table_id)
        |> select([:timestamp, :metadata])
        |> EctoQueryBQ.where_nested_eq(path, value)

      {sql, params} = Ecto.Adapters.SQL.to_sql(:all, Repo, q)

      sql = EctoQueryBQ.ecto_pg_sql_to_bq_sql(sql)

      assert sql ==
               ~s|SELECT t0.timestamp, t0.metadata FROM #{bq_table_id} AS t0 INNER JOIN UNNEST(t0.metadata) AS f1 ON TRUE INNER JOIN UNNEST(f1.user) AS f2 ON TRUE WHERE (f2.id = ?)|

      assert params == [5]

      path = "metadata.user.address.country"
      value = "AQ"

      q =
        from(bq_table_id)
        |> select([:timestamp, :metadata])
        |> EctoQueryBQ.where_nested_eq(path, value)

      {sql, params} = Ecto.Adapters.SQL.to_sql(:all, Repo, q)

      sql = EctoQueryBQ.ecto_pg_sql_to_bq_sql(sql)

      assert sql ==
               ~s|SELECT t0.timestamp, t0.metadata FROM #{bq_table_id} AS t0 INNER JOIN UNNEST(t0.metadata) AS f1 ON TRUE INNER JOIN UNNEST(f1.user) AS f2 ON TRUE INNER JOIN UNNEST(f2.address) AS f3 ON TRUE WHERE (f3.country = ?)|

      assert params == ["AQ"]
    end

    test "multiple paths with various depth levels", %{sources: [source | _], users: [user | _]} do
      bq_table_id = System.get_env("LOGFLARE_DEV_BQ_TABLE_ID_FOR_TESTING")

      pathvalues = [
        %{
          path: "metadata.user.address.country",
          value: "AQ"
        },
        %{
          path: "metadata.user.address.city",
          value: "Aboa"
        },
        %{
          path: "metadata.user.rating",
          value: {">", 100}
        },
        %{
          path: "metadata.user.source_count",
          value: {"<", 10}
        },
        %{
          path: "metadata.user.exceptions",
          value: {">=", 0}
        },
        %{
          path: "metadata.user.variables",
          value: {"<=", 2}
        },
        %{
          path: "metadata.user.name",
          value: {"~", "Neo"}
        },
        %{
          path: "metadata.datacenter",
          value: "AWS"
        },
        %{
          path: "metadata.context.pid",
          value: "<0.255.0>"
        },
        %{
          path: "metadata.context.file",
          value: "lib/bigquery.ex"
        }
      ]

      q =
        from(bq_table_id)
        |> select([:timestamp, :metadata])
        |> EctoQueryBQ.where_nested_eqs(pathvalues)

      {sql, params} = Ecto.Adapters.SQL.to_sql(:all, Repo, q)

      sql = EctoQueryBQ.ecto_pg_sql_to_bq_sql(sql)

      sql =
        Enum.reduce(params, sql, fn param, sql ->
          String.replace(sql, ~S"?", ~s|"#{param}"|, global: false)
        end)

      path = Application.app_dir(:logflare) <> "/generated.sql"
      File.write!(path, sql)

      assert sql ==
               ~s|
               SELECT t0.timestamp, t0.metadata
               FROM #{bq_table_id} AS t0
               INNER JOIN UNNEST(t0.metadata) AS f1 ON TRUE
               INNER JOIN UNNEST(f1.context) AS f2 ON TRUE
               INNER JOIN UNNEST(f2.user) AS f3 ON TRUE
               INNER JOIN UNNEST(f3.address) AS f4 ON TRUE
           WHERE (f1.datacenter = "AWS")
           AND (f2.file = "lib/bigquery.ex")
           AND (f2.pid = "<0.255.0>")
           AND (f4.city = "Aboa")
           AND (f4.country = "AQ")
               |
               |> String.replace(~r/\s+/, " ")
               |> String.trim()

      assert params == ["AWS", "lib/bigquery.ex", "<0.255.0>", "Aboa", "AQ"]
    end
  end

  describe "NestedPath" do
    test "to_map" do
      pathvalues = [
        %{
          path: "metadata.user.address.country",
          value: "AQ"
        },
        %{
          path: "metadata.user.address.city",
          value: "Aboa"
        },
        %{
          path: "metadata.datacenter",
          value: "AWS"
        },
        %{
          path: "metadata.context.pid",
          value: "<0.255.0>"
        },
        %{
          path: "metadata.context.file",
          value: "lib/bigquery.ex"
        }
      ]

      assert EctoQueryBQ.NestedPath.to_map(pathvalues) == %{
               metadata: %{
                 context: %{file: "lib/bigquery.ex", pid: "<0.255.0>"},
                 datacenter: "AWS",
                 user: %{address: %{city: "Aboa", country: "AQ"}}
               }
             }
    end
  end
end
