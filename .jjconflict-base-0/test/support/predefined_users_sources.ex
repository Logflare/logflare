defmodule Logflare.BigQuery.PredefinedTestUser do
  @moduledoc false

  def table_schema do
    %GoogleApi.BigQuery.V2.Model.TableSchema{
      fields: [
        %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
          categories: nil,
          description: nil,
          fields: nil,
          mode: "NULLABLE",
          name: "event_message",
          policyTags: nil,
          type: "STRING"
        },
        %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
          categories: nil,
          description: nil,
          fields: nil,
          mode: "NULLABLE",
          name: "id",
          policyTags: nil,
          type: "STRING"
        },
        %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
          categories: nil,
          description: nil,
          fields: [
            %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
              categories: nil,
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "float_field_1",
              policyTags: nil,
              type: "FLOAT"
            },
            %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
              categories: nil,
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "int_field_1",
              policyTags: nil,
              type: "INTEGER"
            }
          ],
          mode: "REPEATED",
          name: "metadata",
          policyTags: nil,
          type: "RECORD"
        },
        %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
          categories: nil,
          description: nil,
          fields: nil,
          mode: "REQUIRED",
          name: "timestamp",
          policyTags: nil,
          type: "TIMESTAMP"
        }
      ]
    }
  end
end
