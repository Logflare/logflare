defmodule Logflare.Google.BigQuery.Debug do
  def gen_bq_ui_url(user, source_token) when is_binary(source_token) do
    base = "https://console.cloud.google.com/bigquery"

    token = String.replace(source_token, "-", "_")

    project =
      user.bigquery_project_id || Application.get_env(:logflare, Logflare.Google)[:project_id]

    dataset = user.bigquery_dataset_id || "#{user.id}_prod"

    base <>
      "?project=#{project}&p=#{project}&d=#{dataset}&t=#{token}&page=table&authuser=#{user.email}"
  end
end
