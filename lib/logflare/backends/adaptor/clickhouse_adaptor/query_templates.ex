defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor.QueryTemplates do
  @moduledoc """
  Common query templates utilized by the `ClickhouseAdaptor`.
  """

  import Logflare.Guards

  @doc """
  Generates a ClickHouse query statement to provision an ingest table for logs.
  """
  @spec create_log_ingest_table_statement(database :: String.t(), table :: String.t()) ::
          String.t()
  def create_log_ingest_table_statement(database, table)
      when is_non_empty_binary(database) and is_non_empty_binary(table) do
    """
    CREATE TABLE IF NOT EXISTS "#{database}"."#{table}" (
      "id" UUID,
      "event_message" String,
      "body" String,
      "timestamp" DateTime64(6)
    )
    ENGINE MergeTree()
    ORDER BY ("timestamp")
    SETTINGS index_granularity = 8192 SETTINGS flatten_nested=0
    """
  end
end
