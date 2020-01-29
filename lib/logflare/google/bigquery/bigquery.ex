defmodule Logflare.Google.BigQuery do
  @moduledoc """
  Big Query interface.
  """

  require Logger

  @project_id Application.get_env(:logflare, Logflare.Google)[:project_id]
  @dataset_id_append Application.get_env(:logflare, Logflare.Google)[:dataset_id_append]
  @service_account Application.get_env(:logflare, Logflare.Google)[:service_account]

  alias GoogleApi.BigQuery.V2.Api
  alias GoogleApi.BigQuery.V2.Model

  alias Logflare.Google.BigQuery.GenUtils
  import GenUtils, only: [maybe_parse_google_api_result: 1]
  alias Logflare.{Users}
  alias Logflare.Source.BigQuery.SchemaBuilder

  @type ok_err_tup :: {:ok, term} | {:error, term}

  @spec init_table!(integer(), atom, String.t(), integer(), String.t(), String.t()) :: ok_err_tup
  def init_table!(user_id, source, project_id, ttl, dataset_location, dataset_id)
      when is_integer(user_id) and is_atom(source) and is_binary(project_id) and is_integer(ttl) and
             is_binary(dataset_location) and is_binary(dataset_id) do
    case create_dataset(user_id, dataset_id, dataset_location, project_id) do
      {:ok, _} ->
        Logger.info("BigQuery dataset created: #{dataset_id}")

        case create_table(source, dataset_id, project_id, ttl) do
          {:ok, table} ->
            Logger.info("BigQuery table created: #{source}")
            {:ok, table}

          {:error, message} ->
            Logger.error("Init error: #{GenUtils.get_tesla_error_message(message)}")
        end

      {:error, %Tesla.Env{status: 409}} ->
        Logger.info("BigQuery dataset found: #{dataset_id}")

        case create_table(source, dataset_id, project_id, ttl) do
          {:ok, table} ->
            Logger.info("BigQuery table created: #{source}")
            {:ok, table}

          {:error, %Tesla.Env{status: 409}} ->
            Logger.info("BigQuery table existed: #{source}")

          {:error, message} ->
            Logger.error("Init error: #{GenUtils.get_tesla_error_message(message)}")
        end

      {:error, message} ->
        Logger.error(
          "BigQuery dataset create error: #{dataset_id}: #{
            GenUtils.get_tesla_error_message(message)
          }"
        )
    end
  end

  @spec delete_table(atom) :: {:error, Tesla.Env.t()} | {:ok, term}
  def delete_table(source_id) do
    conn = GenUtils.get_conn()
    table_name = GenUtils.format_table_name(source_id)

    %{user_id: user_id, bigquery_project_id: project_id, bigquery_dataset_id: dataset_id} =
      GenUtils.get_bq_user_info(source_id)

    conn
    |> Api.Tables.bigquery_tables_delete(
      project_id,
      dataset_id || Integer.to_string(user_id) <> @dataset_id_append,
      table_name
    )
    |> maybe_parse_google_api_result()
  end

  @spec create_table(atom, binary, binary, any) ::
          {:error, Tesla.Env.t()} | {:ok, Model.Table.t()}
  def create_table(source, dataset_id, project_id, table_ttl) do
    conn = GenUtils.get_conn()
    table_name = GenUtils.format_table_name(source)

    schema = SchemaBuilder.initial_table_schema()

    reference = %Model.TableReference{
      datasetId: dataset_id,
      projectId: project_id,
      tableId: table_name
    }

    partitioning = %Model.TimePartitioning{
      type: "DAY",
      expirationMs: table_ttl
    }

    conn
    |> Api.Tables.bigquery_tables_insert(
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
    |> maybe_parse_google_api_result()
  end

  @spec patch_table_ttl(atom, integer(), binary, binary) :: ok_err_tup
  def patch_table_ttl(source_id, table_ttl, dataset_id, project_id) do
    conn = GenUtils.get_conn()
    table_name = GenUtils.format_table_name(source_id)
    dataset_id = dataset_id || GenUtils.get_account_id(source_id) <> @dataset_id_append

    partitioning = %Model.TimePartitioning{
      type: "DAY",
      expirationMs: table_ttl
    }

    conn
    |> Api.Tables.bigquery_tables_patch(project_id, dataset_id, table_name,
      body: %Model.Table{timePartitioning: partitioning}
    )
    |> maybe_parse_google_api_result()
  end

  @spec patch_table(atom, any, binary, binary) :: {:error, Tesla.Env.t()} | {:ok, Model.Table.t()}
  def patch_table(source_id, schema, dataset_id, project_id) do
    conn = GenUtils.get_conn()
    table_name = GenUtils.format_table_name(source_id)
    dataset_id = dataset_id || GenUtils.get_account_id(source_id) <> @dataset_id_append

    conn
    |> Api.Tables.bigquery_tables_patch(project_id, dataset_id, table_name,
      body: %Model.Table{schema: schema}
    )
    |> maybe_parse_google_api_result()
  end

  @spec get_table(atom) :: {:error, Tesla.Env.t()} | {:ok, term}
  def get_table(source_id) do
    conn = GenUtils.get_conn()
    table_name = GenUtils.format_table_name(source_id)

    %{
      bigquery_project_id: project_id,
      bigquery_dataset_id: dataset_id
    } = GenUtils.get_bq_user_info(source_id)

    dataset_id = dataset_id || GenUtils.get_account_id(source_id) <> @dataset_id_append

    conn
    |> Api.Tables.bigquery_tables_get(
      project_id,
      dataset_id,
      table_name
    )
    |> maybe_parse_google_api_result()
  end

  @spec stream_batch!(atom, list(map)) :: ok_err_tup
  def stream_batch!(source_id, batch) when is_atom(source_id) do
    conn = GenUtils.get_conn()
    table_name = GenUtils.format_table_name(source_id)

    %{
      bigquery_project_id: project_id,
      bigquery_dataset_id: dataset_id
    } = GenUtils.get_bq_user_info(source_id)

    dataset_id = dataset_id || GenUtils.get_account_id(source_id) <> @dataset_id_append

    body = %Model.TableDataInsertAllRequest{
      ignoreUnknownValues: true,
      rows: batch
    }

    conn
    |> Api.Tabledata.bigquery_tabledata_insert_all(
      project_id,
      dataset_id,
      table_name,
      body: body
    )
    |> maybe_parse_google_api_result()
  end

  @doc """
  Creates dataset, accepts user_id, dataset_id, dataset_location, project_id
  """
  @spec create_dataset(integer, binary, binary, binary) ::
          {:error, Tesla.Env.t()} | {:ok, Model.Dataset.t()}
  def create_dataset(user_id, dataset_id, dataset_location, project_id \\ @project_id) do
    conn = GenUtils.get_conn()

    %Logflare.User{email: email, provider: provider} = Users.get_by(id: user_id)

    reference = %Model.DatasetReference{
      datasetId: dataset_id,
      projectId: project_id
    }

    access =
      if provider == "google" do
        [
          %Model.DatasetAccess{
            role: "READER",
            userByEmail: email
          },
          %Model.DatasetAccess{
            role: "WRITER",
            specialGroup: "projectWriters"
          },
          %Model.DatasetAccess{
            role: "OWNER",
            specialGroup: "projectOwners"
          },
          %Model.DatasetAccess{
            role: "OWNER",
            userByEmail: @service_account
          },
          %Model.DatasetAccess{
            role: "READER",
            specialGroup: "projectReaders"
          }
        ]
      else
        []
      end

    body = %Model.Dataset{
      datasetReference: reference,
      access: access,
      description: "Managed by Logflare",
      labels: %{"managed_by" => "logflare"},
      location: dataset_location
    }

    conn
    |> Api.Datasets.bigquery_datasets_insert(project_id, body: body)
    |> maybe_parse_google_api_result()
  end

  @spec patch_dataset_access!(non_neg_integer()) :: ok_err_tup
  def patch_dataset_access!(user_id) do
    conn = GenUtils.get_conn()

    %Logflare.User{
      email: email,
      provider: provider,
      bigquery_dataset_id: dataset_id,
      bigquery_project_id: project_id
    } = Users.get_by(id: user_id)

    dataset_id = dataset_id || Integer.to_string(user_id) <> @dataset_id_append

    Task.Supervisor.start_child(Logflare.TaskSupervisor, fn ->
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
          Api.Datasets.bigquery_datasets_patch(conn, project_id || @project_id, dataset_id,
            body: body
          )

        Logger.info("Dataset patched: #{dataset_id} | #{email}")
      end
    end)
  end

  @doc """
  Deletes dataset for the given user.
  """
  @spec delete_dataset(User.t()) :: ok_err_tup
  def delete_dataset(user) do
    conn = GenUtils.get_conn()
    dataset_id = user.bigquery_dataset_id || Integer.to_string(user.id) <> @dataset_id_append
    project_id = user.bigquery_project_id || @project_id

    conn
    |> Api.Datasets.bigquery_datasets_delete(project_id, dataset_id, deleteContents: true)
    |> maybe_parse_google_api_result()
  end

  def query(%Model.QueryRequest{} = body, opts \\ []) do
    project_id = opts[:project_id] || @project_id
    use_query_cache = opts[:use_query_cache]
    conn = GenUtils.get_conn()

    conn
    |> Api.Jobs.bigquery_jobs_query(
      project_id,
      body: body
    )
    |> maybe_parse_google_api_result()
  end

  def sql_query_with_cache(sql, params \\ [], opts \\ []) when is_binary(sql) do
    project_id = opts[:project_id] || @project_id
    conn = GenUtils.get_conn()

    conn
    |> Api.Jobs.bigquery_jobs_query(
      project_id,
      body: %Model.QueryRequest{
        query: sql,
        useLegacySql: false,
        useQueryCache: true,
        parameterMode: "NAMED",
        queryParameters: params
      }
    )
    |> maybe_parse_google_api_result()
  end
end
