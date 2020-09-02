defmodule Logflare.User.BigQueryUDFs do
  @moduledoc false
  alias __MODULE__.SearchFns, as: SFns
  require Logger
  alias Logflare.BqRepo
  alias Logflare.User
  alias Logflare.Users

  def create_if_not_exists_udfs_for_user_dataset(
        %User{
          bigquery_project_id: bq_project_id,
          bigquery_dataset_id: bq_dataset_id,
          bigquery_udfs_hash: bq_udfs_hash
        } = user
      ) do
    # Ensure that hash changes if SQL changes or dataset_id changes or bq_project_id changes
    sql = full_udf_sql_for_dataset(bq_project_id, bq_dataset_id)

    new_udfs_hash = to_md5_hash(sql)

    if bq_udfs_hash != new_udfs_hash do
      result = BqRepo.query_with_sql_and_params(bq_project_id, sql, [], useQueryCache: false)

      with {:ok, _} <- result,
           {:ok, user} <- Users.update_user_all_fields(user, %{bigquery_udfs_hash: new_udfs_hash}) do
        Logger.info(
          "Created BQ UDFs for dataset #{bq_dataset_id} for project #{bq_project_id} for user #{
            user.id
          }"
        )

        {:ok, user}
      else
        {:error, message} ->
          Logger.error(
            "Error creating BQ UDFs for dataset #{bq_dataset_id} for project #{bq_project_id}: #{
              inspect(message)
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

  def full_udf_sql_for_dataset(bq_project_id, bq_dataset_id)

  def full_udf_sql_for_dataset(bqid, bdid)
      when is_binary(bdid) and bdid != "" and is_binary(bqid) and bqid != "" do
    "
    #{SFns.lf_timestamp_sub(bqid, bdid)}
    #{SFns.lf_timestamp_trunc(bqid, bdid)}
    #{SFns.lf_timestamp_trunc_with_timezone(bqid, bdid)}
    #{SFns.lf_generate_timestamp_array(bqid, bdid)}
    "
  end
end
