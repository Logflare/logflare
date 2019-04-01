defmodule Logflare.BigQuery do
  @moduledoc """
  Big Query interface.
  """

  require Logger

  @project_id "logflare-232118"
  @dataset_id "logflare_dev"

  def init_table(source) do
    Task.Supervisor.start_child(Logflare.TaskSupervisor, fn ->
      case create_table(source) do
        {:ok, _} ->
          Logger.info("BigQuery table created: #{source}")

        {:error, %Tesla.Env{status: 409}} ->
          Logger.info("BigQuery table existed: #{source}")

        {:error, _} ->
          Logger.info("BigQuery init error: #{source}")
      end
    end)
  end

  def delete_table(source) do
    {:ok, token} = Goth.Token.for_scope("https://www.googleapis.com/auth/cloud-platform")
    conn = GoogleApi.BigQuery.V2.Connection.new(token.token)
    table_name = format_table_name(source)

    GoogleApi.BigQuery.V2.Api.Tables.bigquery_tables_delete(
      conn,
      @project_id,
      @dataset_id,
      table_name
    )
  end

  def create_table(source) do
    {:ok, token} = Goth.Token.for_scope("https://www.googleapis.com/auth/cloud-platform")
    conn = GoogleApi.BigQuery.V2.Connection.new(token.token)
    table_name = format_table_name(source)

    schema = %GoogleApi.BigQuery.V2.Model.TableSchema{
      fields: [
        %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
          description: nil,
          fields: nil,
          mode: "REQUIRED",
          name: "timestamp",
          type: "TIMESTAMP"
        },
        %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
          description: nil,
          fields: nil,
          mode: nil,
          name: "log_message",
          type: "STRING"
        }
      ]
    }

    reference = %GoogleApi.BigQuery.V2.Model.TableReference{
      datasetId: @dataset_id,
      projectId: @project_id,
      tableId: table_name
    }

    GoogleApi.BigQuery.V2.Api.Tables.bigquery_tables_insert(
      conn,
      @project_id,
      @dataset_id,
      body: %GoogleApi.BigQuery.V2.Model.Table{schema: schema, tableReference: reference}
    )
  end

  def get_table(source) do
    {:ok, token} = Goth.Token.for_scope("https://www.googleapis.com/auth/cloud-platform")
    conn = GoogleApi.BigQuery.V2.Connection.new(token.token)
    table_name = format_table_name(source)

    GoogleApi.BigQuery.V2.Api.Tables.bigquery_tables_get(
      conn,
      @project_id,
      @dataset_id,
      table_name
    )
  end

  def get_events(source) do
    {:ok, token} = Goth.Token.for_scope("https://www.googleapis.com/auth/cloud-platform")
    conn = GoogleApi.BigQuery.V2.Connection.new(token.token)
    table_name = format_table_name(source)

    sql =
      "SELECT * FROM [#{@project_id}:#{@dataset_id}.#{table_name}] ORDER BY timestamp LIMIT 100"

    {:ok, response} =
      GoogleApi.BigQuery.V2.Api.Jobs.bigquery_jobs_query(
        conn,
        @project_id,
        body: %GoogleApi.BigQuery.V2.Model.QueryRequest{query: sql}
      )

    response.rows
    |> Enum.each(fn row ->
      row.f
      |> Enum.with_index()
      |> Enum.each(fn {cell, i} ->
        IO.puts("#{Enum.at(response.schema.fields, i).name}: #{cell.v}")
      end)
    end)
  end

  def stream_event(source, unix_timestamp, message) do
    {:ok, token} = Goth.Token.for_scope("https://www.googleapis.com/auth/cloud-platform")
    conn = GoogleApi.BigQuery.V2.Connection.new(token.token)
    table_name = format_table_name(source)
    {:ok, timestamp} = DateTime.from_unix(unix_timestamp, :microsecond)
    row_json = %{"timestamp" => timestamp, "log_message" => message}

    row = %GoogleApi.BigQuery.V2.Model.TableDataInsertAllRequestRows{
      insertId: Ecto.UUID.generate(),
      json: row_json
    }

    body = %GoogleApi.BigQuery.V2.Model.TableDataInsertAllRequest{
      rows: [row]
    }

    {:ok, _response} =
      GoogleApi.BigQuery.V2.Api.Tabledata.bigquery_tabledata_insert_all(
        conn,
        @project_id,
        @dataset_id,
        table_name,
        body: body
      )
  end

  def stream_batch!(source, batch) do
    {:ok, token} = Goth.Token.for_scope("https://www.googleapis.com/auth/cloud-platform")
    conn = GoogleApi.BigQuery.V2.Connection.new(token.token)
    table_name = format_table_name(source)

    body = %GoogleApi.BigQuery.V2.Model.TableDataInsertAllRequest{
      rows: batch
    }

    {:ok, _response} =
      GoogleApi.BigQuery.V2.Api.Tabledata.bigquery_tabledata_insert_all(
        conn,
        @project_id,
        @dataset_id,
        table_name,
        body: body
      )
  end

  def format_table_name(source) do
    string = Atom.to_string(source)
    String.replace(string, "-", "_")
  end
end
