defmodule Logflare.Source.BigQuery.UDF do
  @moduledoc false
  alias __MODULE__.SearchFns
  alias Logflare.Google.BigQuery
  alias GoogleApi.BigQuery.V2.Model.QueryRequest
  alias Logflare.User

  def create_default_udfs_for_user!(%{
        bigquery_project_id: bq_project_id,
        bigquery_dataset_id: bq_dataset_id
      }) do
    {:ok, _} =
      BigQuery.query(%QueryRequest{
        query: SearchFns.lf_timestamp_sub(bq_dataset_id),
        useLegacySql: false,
        useQueryCache: false
      })

    {:ok, _} =
      BigQuery.query(%QueryRequest{
        query: SearchFns.lf_timestamp_trunc(bq_dataset_id),
        useLegacySql: false,
        useQueryCache: false
      })

    {:ok, _} =
      BigQuery.query(%QueryRequest{
        query: SearchFns.lf_generate_timestamp_array(bq_dataset_id),
        useLegacySql: false,
        useQueryCache: false
      })
  end
end
