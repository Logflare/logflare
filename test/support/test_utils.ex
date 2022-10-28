defmodule Logflare.TestUtils do
  @moduledoc """
  Testing utilities. Globally alised under the `TestUtils` namespace.
  """
  alias Logflare.Source.BigQuery.SchemaBuilder
  alias GoogleApi.BigQuery.V2.Model.{TableSchema, TableFieldSchema}

  @spec random_string(non_neg_integer()) :: String.t()
  def random_string(length \\ 6) do
    :crypto.strong_rand_bytes(length) |> Base.url_encode64() |> binary_part(0, length)
  end

  def gen_bq_timestamp do
    inspect((DateTime.utc_now() |> DateTime.to_unix(:microsecond)) / 1_000_000_000_000_000) <>
      "E9"
  end

  def gen_uuid do
    Ecto.UUID.generate()
  end

  @doc """
  Generates a mock BigQuery response.
  This is a successful bq response retrieved manually
  """
  def gen_bq_response(result \\ %{})

  def gen_bq_response(result) when is_map(result) do
    gen_bq_response([result])
  end

  def gen_bq_response(results) when is_list(results) do
    results =
      Enum.map(results, fn result ->
        result
        |> Enum.into(%{
          "event_message" => "some event message",
          "timestamp" => gen_bq_timestamp(),
          "id" => gen_uuid()
        })
      end)

    schema = SchemaBuilder.initial_table_schema()

    rows =
      for result <- results do
        row = %GoogleApi.BigQuery.V2.Model.TableRow{}

        cells =
          for field <- schema.fields do
            value = Map.get(result, field.name)
            %GoogleApi.BigQuery.V2.Model.TableCell{v: value}
          end

        %{row | f: cells}
      end

    %GoogleApi.BigQuery.V2.Model.QueryResponse{
      cacheHit: true,
      jobComplete: true,
      jobReference: %GoogleApi.BigQuery.V2.Model.JobReference{
        jobId: "job_eoaOXgp9U0VFOPiOHbX6fIT3z3KU",
        location: "US",
        projectId: "logflare-dev-238720"
      },
      kind: "bigquery#queryResponse",
      rows: rows,
      schema: schema,
      totalBytesProcessed: "0",
      totalRows: inspect(length(results))
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
