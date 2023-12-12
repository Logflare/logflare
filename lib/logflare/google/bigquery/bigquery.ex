defmodule Logflare.Google.BigQuery do
  @moduledoc """
  Big Query interface.
  """

  require Logger

  defp env_project_id, do: Application.get_env(:logflare, Logflare.Google)[:project_id]

  defp env_dataset_id_append,
    do: Application.get_env(:logflare, Logflare.Google)[:dataset_id_append]

  defp env_service_account, do: Application.get_env(:logflare, Logflare.Google)[:service_account]

  alias GoogleApi.BigQuery.V2.Api
  alias GoogleApi.BigQuery.V2.Api.Tabledata
  alias GoogleApi.BigQuery.V2.Api.Tables
  alias GoogleApi.BigQuery.V2.Model

  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Users
  alias Logflare.User
  alias Logflare.Billing
  alias Logflare.Billing.Plan
  alias Logflare.TeamUsers
  alias Logflare.Source.BigQuery.SchemaBuilder
  alias Logflare.Source.RecentLogsServer, as: RLS

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
          "BigQuery dataset create error: #{dataset_id}: #{GenUtils.get_tesla_error_message(message)}"
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
      dataset_id || Integer.to_string(user_id) <> env_dataset_id_append(),
      table_name
    )
    |> GenUtils.maybe_parse_google_api_result()
  end

  @spec create_table(atom, binary, binary, any) ::
          {:error, Tesla.Env.t()} | {:ok, Model.Table.t()}
  def create_table(source, dataset_id, project_id, table_ttl) when is_atom(source) do
    conn = GenUtils.get_conn()
    table_name = GenUtils.format_table_name(source)

    schema = SchemaBuilder.initial_table_schema()

    reference = %Model.TableReference{
      datasetId: dataset_id,
      projectId: project_id,
      tableId: table_name
    }

    partitioning = %Model.TimePartitioning{
      requirePartitionFilter: true,
      field: "timestamp",
      type: "DAY",
      expirationMs: table_ttl
    }

    clustering = %Model.Clustering{
      fields: ["timestamp", "id"]
    }

    conn
    |> Api.Tables.bigquery_tables_insert(
      project_id,
      dataset_id,
      body: %Model.Table{
        schema: schema,
        tableReference: reference,
        requirePartitionFilter: true,
        timePartitioning: partitioning,
        clustering: clustering,
        description: "Managed by Logflare",
        labels: %{
          "managed_by" => "logflare",
          "logflare_source" => GenUtils.format_key(source)
        }
      }
    )
    |> GenUtils.maybe_parse_google_api_result()
  end

  @spec patch_table_ttl(atom, integer(), binary, binary) :: ok_err_tup
  def patch_table_ttl(source_id, table_ttl, dataset_id, project_id) do
    conn = GenUtils.get_conn()
    table_name = GenUtils.format_table_name(source_id)
    dataset_id = dataset_id || GenUtils.get_account_id(source_id) <> env_dataset_id_append()

    partitioning = %Model.TimePartitioning{
      type: "DAY",
      expirationMs: table_ttl
    }

    conn
    |> Api.Tables.bigquery_tables_patch(project_id, dataset_id, table_name,
      body: %Model.Table{timePartitioning: partitioning}
    )
    |> GenUtils.maybe_parse_google_api_result()
  end

  @spec patch_table(atom, any, binary, binary) :: {:error, Tesla.Env.t()} | {:ok, Model.Table.t()}
  def patch_table(source_id, schema, dataset_id, project_id) do
    conn = GenUtils.get_conn()
    table_name = GenUtils.format_table_name(source_id)
    dataset_id = dataset_id || GenUtils.get_account_id(source_id) <> env_dataset_id_append()

    conn
    |> Tables.bigquery_tables_patch(project_id, dataset_id, table_name,
      body: %Model.Table{schema: schema}
    )
    |> GenUtils.maybe_parse_google_api_result()
  end

  @spec get_table(atom) :: {:error, Tesla.Env.t()} | {:ok, term}
  def get_table(source_id) do
    conn = GenUtils.get_conn()
    table_name = GenUtils.format_table_name(source_id)

    %{
      bigquery_project_id: project_id,
      bigquery_dataset_id: dataset_id
    } = GenUtils.get_bq_user_info(source_id)

    dataset_id = dataset_id || GenUtils.get_account_id(source_id) <> env_dataset_id_append()

    conn
    |> Api.Tables.bigquery_tables_get(
      project_id,
      dataset_id,
      table_name
    )
    |> GenUtils.maybe_parse_google_api_result()
  end

  def stream_batch!(
        %RLS{
          bigquery_project_id: project_id,
          bigquery_dataset_id: dataset_id,
          source_id: source_id
        },
        batch
      )
      when is_atom(source_id) do
    conn = GenUtils.get_conn(:ingest)
    table_name = GenUtils.format_table_name(source_id)

    body = %Model.TableDataInsertAllRequest{
      ignoreUnknownValues: true,
      rows: batch
    }

    conn
    |> Tabledata.bigquery_tabledata_insert_all(
      project_id,
      dataset_id,
      table_name,
      body: body
    )
    |> GenUtils.maybe_parse_google_api_result()
  end

  @doc """
  Creates dataset, accepts user_id, dataset_id, dataset_location, project_id
  """
  @spec create_dataset(integer, binary, binary, binary) ::
          {:error, Tesla.Env.t()} | {:ok, Model.Dataset.t()}
  def create_dataset(user_id, dataset_id, dataset_location, project_id \\ env_project_id()) do
    conn = GenUtils.get_conn()

    %User{email: email, provider: provider} = user = Users.get_by(id: user_id)
    %Plan{name: plan} = Billing.Cache.get_plan_by_user(user)

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
            userByEmail: env_service_account()
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
      labels: %{
        "managed_by" => "logflare",
        "logflare_plan" => GenUtils.format_key(plan),
        "logflare_account" => GenUtils.format_key(user.id)
      },
      location: dataset_location
    }

    conn
    |> Api.Datasets.bigquery_datasets_insert(project_id, body: body)
    |> GenUtils.maybe_parse_google_api_result()
  end

  def patch_dataset_access(
        %User{
          bigquery_dataset_id: dataset_id,
          bigquery_project_id: project_id
        } = user
      ) do
    user =
      user
      |> Users.preload_sources()
      |> Users.preload_team()

    team_users = if user.team, do: TeamUsers.list_team_users_by(team_id: user.team.id), else: []

    emails =
      for x <- [user | team_users], x.provider == "google", do: x.email

    if Enum.count(user.sources) > 0 do
      Task.Supervisor.start_child(Logflare.TaskSupervisor, fn ->
        patch(dataset_id, emails, project_id, user.id)
      end)

      {:ok, :patch_attempted}
    else
      {:ok, :nothing_patched}
    end
  end

  @doc """
  Deletes dataset for the given user.
  """
  @spec delete_dataset(User.t()) :: ok_err_tup
  def delete_dataset(user) do
    conn = GenUtils.get_conn()
    dataset_id = user.bigquery_dataset_id || Integer.to_string(user.id) <> env_dataset_id_append()
    project_id = user.bigquery_project_id || env_project_id()

    conn
    |> Api.Datasets.bigquery_datasets_delete(project_id, dataset_id, deleteContents: true)
    |> GenUtils.maybe_parse_google_api_result()
  end

  def patch_dataset_labels(%User{} = user) do
    conn = GenUtils.get_conn()
    dataset_id = user.bigquery_dataset_id || Integer.to_string(user.id) <> env_dataset_id_append()
    project_id = user.bigquery_project_id || env_project_id()

    %Plan{name: plan} =
      user
      |> Billing.Cache.get_plan_by_user()

    body = %Model.Dataset{
      description: "Managed by Logflare",
      labels: %{
        "managed_by" => "logflare",
        "logflare_plan" => GenUtils.format_key(plan),
        "logflare_account" => GenUtils.format_key(user.id)
      }
    }

    case Api.Datasets.bigquery_datasets_patch(
           conn,
           project_id,
           dataset_id,
           body: body
         ) do
      {:ok, %GoogleApi.BigQuery.V2.Model.Dataset{}} ->
        Logger.info("Dataset labels patched: #{dataset_id}")
        {:ok, :patched}

      {:ok, response} ->
        Logger.info("Dataset labels NOT patched: #{dataset_id}", error_string: inspect(response))
        {:error, :not_patched}

      {:error, response} ->
        Logger.warning("Dataset labels NOT patched: #{dataset_id}",
          error_string: inspect(response)
        )

        {:error, :not_patched}
    end
  end

  defp patch(_dataset_id, [], _project_id, _user_id), do: :noop

  defp patch(dataset_id, emails, project_id, user_id) do
    conn = GenUtils.get_conn()
    dataset_id = dataset_id || Integer.to_string(user_id) <> env_dataset_id_append()

    access_emails =
      Enum.map(emails, fn x ->
        %GoogleApi.BigQuery.V2.Model.DatasetAccess{
          role: "READER",
          userByEmail: x
        }
      end)

    access_defaults = [
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
        userByEmail: env_service_account()
      },
      %GoogleApi.BigQuery.V2.Model.DatasetAccess{
        role: "READER",
        specialGroup: "projectReaders"
      }
    ]

    access = access_emails ++ access_defaults

    %Plan{name: plan} =
      Users.Cache.get_by(id: user_id)
      |> Billing.Cache.get_plan_by_user()

    body = %Model.Dataset{
      access: access,
      description: "Managed by Logflare",
      labels: %{
        "managed_by" => "logflare",
        "logflare_plan" => GenUtils.format_key(plan),
        "logflare_account" => GenUtils.format_key(user_id)
      }
    }

    {:ok, _response} =
      Api.Datasets.bigquery_datasets_patch(conn, project_id || env_project_id(), dataset_id,
        body: body
      )

    Logger.info("Dataset patched: #{dataset_id} | #{inspect(emails)}")
  end
end
