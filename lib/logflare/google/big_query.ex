defmodule Logflare.Google.BigQuery do
  @moduledoc """
  Big Query interface.
  """

  require Logger

  @project_id Application.get_env(:logflare, Logflare.Google)[:project_id] || ""
  @dataset_id_append Application.get_env(:logflare, Logflare.Google)[:dataset_id_append] || ""
  @service_account Application.get_env(:logflare, Logflare.Google)[:service_account]

  alias GoogleApi.BigQuery.V2.Api
  alias GoogleApi.BigQuery.V2.Model
  alias GoogleApi.BigQuery.V2.Connection
  alias Logflare.{Sources, Users}

  @table_ttl 604_800_000
  # seven days
  @type ok_err_tup :: {:ok, term} | {:error, term}

  @spec init_table!(atom, String.t(), integer()) :: ok_err_tup
  def init_table!(source, project_id, ttl) do
    dataset_id = get_account_id(source)

    case create_dataset(dataset_id, project_id) do
      {:ok, _} ->
        Logger.info("BigQuery dataset created: #{dataset_id}")

        case create_table(source, project_id, ttl) do
          {:ok, _} ->
            Logger.info("BigQuery table created: #{source}")

          {:error, message} ->
            Logger.error("Init error: #{message.body}")
        end

      {:error, %Tesla.Env{status: 409}} ->
        Logger.info("BigQuery dataset found: #{dataset_id}")

        case create_table(source, project_id, ttl) do
          {:ok, _} ->
            Logger.info("BigQuery table created: #{source}")

          {:error, %Tesla.Env{status: 409}} ->
            Logger.info("BigQuery table existed: #{source}")

          {:error, message} ->
            Logger.error("Init error: #{message.body}")
        end

      {:error, message} ->
        Logger.error("Init error: #{message.body}")
    end
  end

  @spec delete_table(atom, atom) :: ok_err_tup
  def delete_table(source, project_id \\ @project_id) do
    conn = get_conn()
    table_name = format_table_name(source)
    dataset_id = get_account_id(source) <> @dataset_id_append

    Api.Tables.bigquery_tables_delete(
      conn,
      project_id,
      dataset_id,
      table_name
    )
  end

  @spec create_table(atom, atom) :: ok_err_tup
  def create_table(source, project_id \\ @project_id, table_ttl \\ @table_ttl) do
    conn = get_conn()
    table_name = format_table_name(source)
    dataset_id = get_account_id(source) <> @dataset_id_append

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
      projectId: project_id,
      tableId: table_name
    }

    partitioning = %Model.TimePartitioning{
      type: "DAY",
      expirationMs: table_ttl
    }

    Api.Tables.bigquery_tables_insert(
      conn,
      project_id,
      dataset_id,
      body: %Model.Table{
        schema: schema,
        tableReference: reference,
        timePartitioning: partitioning,
        description: "Managed by Logflare",
        labels: %{"managed_by" => "logflare"}
      }
    )
  end

  @spec patch_table_ttl(atom, integer()) :: ok_err_tup
  def patch_table_ttl(source, table_ttl, project_id \\ @project_id) do
    conn = get_conn()
    table_name = format_table_name(source)
    dataset_id = get_account_id(source) <> @dataset_id_append

    partitioning = %Model.TimePartitioning{
      type: "DAY",
      expirationMs: table_ttl
    }

    Api.Tables.bigquery_tables_patch(conn, project_id, dataset_id, table_name,
      body: %Model.Table{timePartitioning: partitioning}
    )
  end

  @spec patch_table(atom, struct(), atom) :: ok_err_tup
  def patch_table(source, schema, project_id \\ @project_id) do
    conn = get_conn()
    table_name = format_table_name(source)
    dataset_id = get_account_id(source) <> @dataset_id_append

    Api.Tables.bigquery_tables_patch(conn, project_id, dataset_id, table_name,
      body: %Model.Table{schema: schema}
    )
  end

  @spec get_table(atom, atom) :: ok_err_tup
  def get_table(source, project_id \\ @project_id) do
    conn = get_conn()
    table_name = format_table_name(source)
    dataset_id = get_account_id(source) <> @dataset_id_append

    Api.Tables.bigquery_tables_get(
      conn,
      project_id,
      dataset_id,
      table_name
    )
  end

  @spec get_events(atom, atom) :: ok_err_tup
  def get_events(source, project_id \\ @project_id) do
    conn = get_conn()
    table_name = format_table_name(source)
    dataset_id = get_account_id(source) <> @dataset_id_append

    sql = "SELECT * FROM [#{project_id}:#{dataset_id}.#{table_name}] ORDER BY timestamp LIMIT 100"

    {:ok, response} =
      Api.Jobs.bigquery_jobs_query(
        conn,
        project_id,
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

  @spec stream_event(atom, integer, tuple, atom) :: ok_err_tup
  def stream_event(source, unix_timestamp, message, project_id \\ @project_id) do
    conn = get_conn()
    table_name = format_table_name(source)
    {:ok, timestamp} = DateTime.from_unix(unix_timestamp, :microsecond)
    row_json = %{"timestamp" => timestamp, "event_message" => message}
    dataset_id = get_account_id(source) <> @dataset_id_append

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
        project_id,
        dataset_id,
        table_name,
        body: body
      )
  end

  @spec stream_batch!(atom, list(map), atom) :: ok_err_tup
  def stream_batch!(source, batch, project_id \\ @project_id) do
    conn = get_conn()
    table_name = format_table_name(source)
    dataset_id = get_account_id(source) <> @dataset_id_append

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
        project_id,
        dataset_id,
        table_name,
        body: body
      )
  end

  @spec create_dataset(String.t(), atom) :: ok_err_tup
  def create_dataset(dataset_id, project_id \\ @project_id) do
    conn = get_conn()
    user_id = String.to_integer(dataset_id)
    %Logflare.User{email: email, provider: provider} = Users.Cache.get_by_id(user_id)

    reference = %Model.DatasetReference{
      datasetId: dataset_id <> @dataset_id_append,
      projectId: project_id
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
            userByEmail: @service_account
          },
          %GoogleApi.BigQuery.V2.Model.DatasetAccess{
            role: "READER",
            specialGroup: "projectReaders"
          }
        ]

        body = %Model.Dataset{
          datasetReference: reference,
          access: access,
          description: "Managed by Logflare",
          labels: %{"managed_by" => "logflare"}
        }

        Api.Datasets.bigquery_datasets_insert(conn, project_id, body: body)

      _ ->
        body = %Model.Dataset{
          datasetReference: reference
        }

        Api.Datasets.bigquery_datasets_insert(conn, project_id, body: body)
    end
  end

  @spec patch_dataset_access!(Integer, atom) :: ok_err_tup
  def patch_dataset_access!(user_id, project_id \\ @project_id) do
    conn = get_conn()
    dataset_id = Integer.to_string(user_id) <> @dataset_id_append

    Task.Supervisor.start_child(Logflare.TaskSupervisor, fn ->
      %Logflare.User{email: email, provider: provider} = Users.Cache.get_by_id(user_id)

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
            userByEmail: @service_account
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
          Api.Datasets.bigquery_datasets_patch(conn, project_id, dataset_id, body: body)

        Logger.info("Dataset patched: #{dataset_id} | #{email}")
      end
    end)
  end

  @spec delete_dataset(integer, atom) :: ok_err_tup
  def delete_dataset(account_id, project_id \\ @project_id) do
    conn = get_conn()
    dataset_id = Integer.to_string(account_id) <> @dataset_id_append

    Api.Datasets.bigquery_datasets_delete(conn, project_id, dataset_id, deleteContents: true)
  end

  @spec list_datasets(atom) :: list(String.t())
  def list_datasets(project_id \\ @project_id) do
    conn = get_conn()
    Api.Datasets.bigquery_datasets_list(conn, project_id)
  end

  @spec get_dataset(integer) :: String.t()
  def get_dataset(account_id, project_id \\ @project_id) do
    dataset_id = "#{account_id}" <> @dataset_id_append
    conn = get_conn()
    Api.Datasets.bigquery_datasets_get(conn, project_id, dataset_id)
  end

  @spec format_table_name(atom) :: String.t()
  def format_table_name(source) do
    string = Atom.to_string(source)
    String.replace(string, "-", "_")
  end

  defp get_conn() do
    {:ok, token} = Goth.Token.for_scope("https://www.googleapis.com/auth/cloud-platform")
    Connection.new(token.token)
  end

  @spec get_account_id(atom) :: String.t()
  def get_account_id(source_id) do
    %Logflare.Source{user_id: account_id} = Sources.Cache.get_by_id(source_id)
    "#{account_id}"
  end
end
