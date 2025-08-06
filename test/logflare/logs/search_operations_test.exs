defmodule Logflare.Logs.SearchOperationsTest do
  use Logflare.DataCase, async: true

  alias Logflare.Logs.SearchOperations
  alias GoogleApi.BigQuery.V2.Model.TableFieldSchema, as: TFS

  @metadata_field %TFS{
    type: "RECORD",
    name: "metadata",
    mode: "REPEATED",
    fields: [
      %TFS{
        type: "STRING",
        name: "status",
        mode: "NULLABLE"
      }
    ]
  }

  describe "unnesting metadata if present" do
    setup do
      schema = Logflare.Source.BigQuery.SchemaBuilder.initial_table_schema()
      source = build(:source, bq_table_id: "1", bq_table_schema: schema)

      [
        so: %Logflare.Logs.SearchOperation{
          source: source,
          querystring: "SELECT * FROM test",
          chart_data_shape_id: nil,
          tailing?: false,
          partition_by: :timestamp
        }
      ]
    end

    test "unnest_log_level/1", %{so: so} do
      so =
        so
        |> SearchOperations.apply_query_defaults()
        |> SearchOperations.unnest_log_level()

      {sql, _} = Logflare.EctoQueryBQ.SQL.to_sql_params(so.query)

      refute sql =~ "UNNEST(t0.metadata)"
    end

    test "unnest_log_level/1 with metadata", %{so: so} do
      metadata_schema =
        so.source.bq_table_schema
        |> Map.put(:fields, so.source.bq_table_schema.fields ++ [@metadata_field])

      so =
        %{so | source: %{so.source | bq_table_schema: metadata_schema}}
        |> SearchOperations.apply_query_defaults()
        |> SearchOperations.unnest_log_level()

      {sql, _} = Logflare.EctoQueryBQ.SQL.to_sql_params(so.query)

      assert sql =~ "UNNEST(t0.metadata)"
    end
  end
end
