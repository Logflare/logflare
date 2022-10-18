defmodule Logflare.TestUtils do
  @moduledoc """
  Testing utilities. Globally alised under the `TestUtils` namespace.
  """
  alias GoogleApi.BigQuery.V2.Model.{TableSchema, TableFieldSchema}

  @spec random_string(non_neg_integer()) :: String.t()
  def random_string(length \\ 6) do
    :crypto.strong_rand_bytes(length) |> Base.url_encode64() |> binary_part(0, length)
  end

  # this is a successful bq response retrieved manually
  @doc """
  Generates a mock BigQuery response.
  """
  def gen_bq_response(type, event_message \\ "some event message")

  def gen_bq_response(:source, event_message) do
    %GoogleApi.BigQuery.V2.Model.QueryResponse{
      cacheHit: true,
      errors: nil,
      jobComplete: true,
      jobReference: %GoogleApi.BigQuery.V2.Model.JobReference{
        jobId: "job_eoaOXgp9U0VFOPiOHbX6fIT3z3KU",
        location: "US",
        projectId: "logflare-dev-238720"
      },
      kind: "bigquery#queryResponse",
      numDmlAffectedRows: nil,
      pageToken: nil,
      rows: [
        %GoogleApi.BigQuery.V2.Model.TableRow{
          f: [
            %GoogleApi.BigQuery.V2.Model.TableCell{v: "1.664961464178735E9"},
            %GoogleApi.BigQuery.V2.Model.TableCell{
              v: "923dc120-e683-42f5-8839-f70e67d9274c"
            },
            %GoogleApi.BigQuery.V2.Model.TableCell{
              v: event_message
            }
          ]
        }
      ],
      schema: %GoogleApi.BigQuery.V2.Model.TableSchema{
        fields: [
          %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
            categories: nil,
            description: nil,
            fields: nil,
            mode: "NULLABLE",
            name: "timestamp",
            policyTags: nil,
            type: "TIMESTAMP"
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
            fields: nil,
            mode: "NULLABLE",
            name: "event_message",
            policyTags: nil,
            type: "STRING"
          }
        ]
      },
      totalBytesProcessed: "0",
      totalRows: "0"
    }
  end

  def gen_bq_response(date, _event_message) when is_binary(date) do
    %GoogleApi.BigQuery.V2.Model.QueryResponse{
      cacheHit: false,
      errors: nil,
      jobComplete: true,
      jobReference: %GoogleApi.BigQuery.V2.Model.JobReference{
        jobId: "job_0rQLvVW-T5P3wSz1CnHRamZj0MiM",
        location: "US",
        projectId: "logflare-dev-238720"
      },
      kind: "bigquery#queryResponse",
      numDmlAffectedRows: nil,
      pageToken: nil,
      rows: [
        %GoogleApi.BigQuery.V2.Model.TableRow{
          f: [%GoogleApi.BigQuery.V2.Model.TableCell{v: date}]
        }
      ],
      schema: %GoogleApi.BigQuery.V2.Model.TableSchema{
        fields: [
          %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
            categories: nil,
            description: nil,
            fields: nil,
            mode: "NULLABLE",
            name: "date",
            policyTags: nil,
            type: "DATE"
          }
        ]
      },
      totalBytesProcessed: "0",
      totalRows: "1"
    }
  end

  @doc """
  Used to retrieve a nested BigQuery field schema from a table schema. Returns nil if not found.

  ### Example
    iex> get_bq_field_schema(%TableSchema{...}, "metadata.a.b")
    %TableFieldSchema{...}
  """
  @spec get_bq_field_schema(TableSchema.t(), String.t()) :: nil | TableFieldSchema.t()
  def get_bq_field_schema(%TableSchema{} = schema, str_path) when is_binary(str_path) do
    str_path
    |> String.split(".")
    |> Enum.reduce(schema, fn
      _key, nil ->
        nil

      key, %_{fields: fields} ->
        Enum.find(fields, fn field -> field.name == key end)
    end)
  end
end
