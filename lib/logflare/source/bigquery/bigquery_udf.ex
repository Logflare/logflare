defmodule Logflare.User.BigQueryUDFs do
  @moduledoc false
  alias __MODULE__.SearchFns, as: SFns
  require Logger
  alias Logflare.Google.BigQuery
  alias GoogleApi.BigQuery.V2.Model.QueryRequest
  alias Logflare.Users
  alias Logflare.User

  def create_if_not_exists_udfs_for_user_dataset(
        %User{
          bigquery_project_id: bq_project_id,
          bigquery_dataset_id: bq_dataset_id,
          bigquery_udfs_hash: bq_udfs_hash
        } = user
      ) do
    sql = full_udf_sql_for_dataset(bq_dataset_id)
    new_udfs_hash = to_md5_hash(sql)

    if bq_udfs_hash != new_udfs_hash do
      result =
        BigQuery.query(%QueryRequest{
          query: sql,
          useLegacySql: false,
          useQueryCache: false
        })

      with {:ok, _} <- result,
           {:ok, _} <- Users.update_user_all_fields(user, %{bigquery_udfs_hash: new_udfs_hash}) do
        Logger.info(
          "Created BQ UDFs for dataset #{bq_dataset_id} for project #{bq_project_id} for user #{
            user.id
          }"
        )

        :ok
      else
        {:error, message} ->
          Logger.error(
            "Error creating BQ UDFs for dataset #{bq_dataset_id} for project #{bq_project_id}: #{
              message
            }"
          )

          {:error, message}
      end
    else
      :noop
    end
  end

  def to_md5_hash(string) when is_binary(string) do
    :crypto.hash(:md5, string) |> Base.encode16(case: :lower)
  end

  def full_udf_sql_for_dataset(bq_dataset_id)

  def full_udf_sql_for_dataset(bdi) when is_binary(bdi) and bdi != "" do
    "
    #{SFns.lf_timestamp_sub(bdi)}
    #{SFns.lf_timestamp_trunc(bdi)}
    #{SFns.lf_generate_timestamp_array(bdi)}
    "
  end
end
