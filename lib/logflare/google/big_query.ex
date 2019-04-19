defmodule Logflare.Google.BigQuery do
  @moduledoc """
  Big Query interface.
  """

  require Logger

  @project_id "logflare-232118"
  @dataset_id_append Application.get_env(:logflare, Logflare.BigQuery)[:dataset_id_append]

  alias GoogleApi.BigQuery.V2.Api
  alias GoogleApi.BigQuery.V2.Model
  alias GoogleApi.BigQuery.V2.Connection
  alias Logflare.Repo
  alias Logflare.Source
  alias Logflare.User

  @table_ttl 604_800_000
  # seven days

  @spec init_table!(:atom) :: {}
  def init_table!(source) do
    dataset_id = get_account_id!(source)

    case create_dataset(dataset_id) do
      {:ok, _} ->
        Logger.info("BigQuery dataset created: #{dataset_id}")
        {:ok, _} = create_table(source)
        Logger.info("BigQuery table created: #{source}")

      {:error, %Tesla.Env{status: 409}} ->
        Logger.info("BigQuery dataset found: #{dataset_id}")

        case create_table(source) do
          {:ok, _} ->
            Logger.info("BigQuery table created: #{source}")

          {:error, %Tesla.Env{status: 409}} ->
            Logger.info("BigQuery table existed: #{source}")
        end
    end
  end

  @spec delete_table(:atom) :: {}
  def delete_table(source) do
    conn = get_conn()
    table_name = format_table_name(source)
    dataset_id = get_account_id!(source) <> @dataset_id_append

    Api.Tables.bigquery_tables_delete(
      conn,
      @project_id,
      dataset_id,
      table_name
    )
  end

  @spec create_table(:atom) :: {}
  def create_table(source) do
    conn = get_conn()
    table_name = format_table_name(source)
    dataset_id = get_account_id!(source) <> @dataset_id_append

    schema = %Model.TableSchema{
      fields: [
        %Model.TableFieldSchema{
          description: nil,
          fields: nil,
          mode: "REQUIRED",
          name: "timestamp",
          type: "TIMESTAMP"
        },
        %Model.TableFieldSchema{
          description: nil,
          fields: nil,
          mode: "NULLABLE",
          name: "event_message",
          type: "STRING"
        }
      ]
    }

    reference = %Model.TableReference{
      datasetId: dataset_id,
      projectId: @project_id,
      tableId: table_name
    }

    partitioning = %Model.TimePartitioning{
      type: "DAY",
      expirationMs: @table_ttl
    }

    Api.Tables.bigquery_tables_insert(
      conn,
      @project_id,
      dataset_id,
      body: %Model.Table{
        schema: schema,
        tableReference: reference,
        timePartitioning: partitioning
      }
    )
  end

  @spec patch_table(:atom, Struct) :: {}
  def patch_table(source, schema) do
    conn = get_conn()
    table_name = format_table_name(source)
    dataset_id = get_account_id!(source) <> @dataset_id_append

    Api.Tables.bigquery_tables_patch(conn, @project_id, dataset_id, table_name,
      body: %Model.Table{schema: schema}
    )
  end

  @spec get_table(:atom) :: {}
  def get_table(source) do
    conn = get_conn()
    table_name = format_table_name(source)
    dataset_id = get_account_id!(source) <> @dataset_id_append

    Api.Tables.bigquery_tables_get(
      conn,
      @project_id,
      dataset_id,
      table_name
    )
  end

  @spec get_table(:atom) :: {}
  def get_events(source) do
    conn = get_conn()
    table_name = format_table_name(source)
    dataset_id = get_account_id!(source) <> @dataset_id_append

    sql =
      "SELECT * FROM [#{@project_id}:#{dataset_id}.#{table_name}] ORDER BY timestamp LIMIT 100"

    {:ok, response} =
      Api.Jobs.bigquery_jobs_query(
        conn,
        @project_id,
        body: %Model.QueryRequest{query: sql}
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

  @spec stream_event(:atom, integer, {}) :: {}
  def stream_event(source, unix_timestamp, message) do
    conn = get_conn()
    table_name = format_table_name(source)
    {:ok, timestamp} = DateTime.from_unix(unix_timestamp, :microsecond)
    row_json = %{"timestamp" => timestamp, "event_message" => message}
    dataset_id = get_account_id!(source) <> @dataset_id_append

    row = %Model.TableDataInsertAllRequestRows{
      insertId: Ecto.UUID.generate(),
      json: row_json
    }

    body = %Model.TableDataInsertAllRequest{
      rows: [row]
    }

    {:ok, _response} =
      Api.Tabledata.bigquery_tabledata_insert_all(
        conn,
        @project_id,
        dataset_id,
        table_name,
        body: body
      )
  end

  @spec stream_batch!(:atom, []) :: {}
  def stream_batch!(source, batch) do
    conn = get_conn()
    table_name = format_table_name(source)
    dataset_id = get_account_id!(source) <> @dataset_id_append

    body = %Model.TableDataInsertAllRequest{
      ignoreUnknownValues: true,
      rows: batch
    }

    {:ok,
     %Model.TableDataInsertAllResponse{
       insertErrors: nil,
       kind: _kind
     }} =
      Api.Tabledata.bigquery_tabledata_insert_all(
        conn,
        @project_id,
        dataset_id,
        table_name,
        body: body
      )
  end

  @spec create_dataset(String.t()) :: {}
  def create_dataset(dataset_id) do
    conn = get_conn()
    user_id = String.to_integer(dataset_id)
    %Logflare.User{email: email, provider: provider} = Repo.get(User, user_id)

    reference = %Model.DatasetReference{
      datasetId: dataset_id <> @dataset_id_append,
      projectId: @project_id
    }

    case provider do
      "google" ->
        access = [
          %GoogleApi.BigQuery.V2.Model.DatasetAccess{
            role: "READER",
            userByEmail: email
          },
          %GoogleApi.BigQuery.V2.Model.DatasetAccess{
            role: "WRITER",
            specialGroup: "projectWriters"
          },
          %GoogleApi.BigQuery.V2.Model.DatasetAccess{
            role: "OWNER",
            specialGroup: "projectOwners"
          },
          %GoogleApi.BigQuery.V2.Model.DatasetAccess{
            role: "OWNER",
            userByEmail: "logflare@logflare-232118.iam.gserviceaccount.com"
          },
          %GoogleApi.BigQuery.V2.Model.DatasetAccess{
            role: "READER",
            specialGroup: "projectReaders"
          }
        ]

        body = %Model.Dataset{
          datasetReference: reference,
          access: access
        }

        Api.Datasets.bigquery_datasets_insert(conn, @project_id, body: body)

      _ ->
        body = %Model.Dataset{
          datasetReference: reference
        }

        Api.Datasets.bigquery_datasets_insert(conn, @project_id, body: body)
    end
  end

  @spec patch_dataset_access!(Integer) :: {}
  def patch_dataset_access!(user_id) do
    conn = get_conn()
    dataset_id = Integer.to_string(user_id) <> @dataset_id_append

    Task.Supervisor.start_child(Logflare.TaskSupervisor, fn ->
      %Logflare.User{email: email, provider: provider} = Repo.get(User, user_id)

      if provider == "google" do
        access = [
          %GoogleApi.BigQuery.V2.Model.DatasetAccess{
            role: "READER",
            userByEmail: email
          },
          %GoogleApi.BigQuery.V2.Model.DatasetAccess{
            role: "WRITER",
            specialGroup: "projectWriters"
          },
          %GoogleApi.BigQuery.V2.Model.DatasetAccess{
            role: "OWNER",
            specialGroup: "projectOwners"
          },
          %GoogleApi.BigQuery.V2.Model.DatasetAccess{
            role: "OWNER",
            userByEmail: "logflare@logflare-232118.iam.gserviceaccount.com"
          },
          %GoogleApi.BigQuery.V2.Model.DatasetAccess{
            role: "READER",
            specialGroup: "projectReaders"
          }
        ]

        body = %Model.Dataset{
          access: access
        }

        {:ok, _response} =
          Api.Datasets.bigquery_datasets_patch(conn, @project_id, dataset_id, body: body)

        Logger.info("Dataset patched: #{dataset_id} | #{email}")
      end
    end)
  end

  @spec delete_dataset(integer) :: {}
  def delete_dataset(account_id) do
    conn = get_conn()
    dataset_id = Integer.to_string(account_id) <> @dataset_id_append

    Api.Datasets.bigquery_datasets_delete(conn, @project_id, dataset_id, deleteContents: true)
  end

  def list_datasets() do
    conn = get_conn()
    Api.Datasets.bigquery_datasets_list(conn, @project_id)
  end

  @spec get_dataset(integer) :: String.t()
  def get_dataset(account_id) do
    dataset_id = "#{account_id}" <> @dataset_id_append
    conn = get_conn()
    Api.Datasets.bigquery_datasets_get(conn, @project_id, dataset_id)
  end

  @spec format_table_name(:atom) :: String.t()
  defp format_table_name(source) do
    string = Atom.to_string(source)
    String.replace(string, "-", "_")
  end

  defp get_conn() do
    {:ok, token} = Goth.Token.for_scope("https://www.googleapis.com/auth/cloud-platform")
    Connection.new(token.token)
  end

  @spec get_account_id!(:atom) :: String.t()
  def get_account_id!(source) do
    %Logflare.Source{user_id: account_id} = Repo.get_by(Source, token: Atom.to_string(source))
    "#{account_id}"
  end
end
