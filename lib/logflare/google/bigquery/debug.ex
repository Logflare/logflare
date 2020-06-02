defmodule Logflare.Google.BigQuery.Debug do
  def gen_bq_ui_url(user_id, source_token) do
    base = "https://console.cloud.google.com/bigquery"
    token = String.replace(source_token, "-", "_")

    base <> "?project=logflare-232118&p=logflare-232118&d=#{user_id}_prod}&t=#{token}&page=table"
  end
end
