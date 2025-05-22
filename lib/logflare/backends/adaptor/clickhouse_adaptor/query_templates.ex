defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor.QueryTemplates do
  @moduledoc """
  Common query templates utilized by the `ClickhouseAdaptor`.
  """

  import Logflare.Guards

  @doc """
  Generates a ClickHouse query statement to provision an ingest table for logs.

  ###Options

  - `:database` - (Optional) Will produce a fully qualified `<database>.<table>` string when provided with a value. Defaults to `nil`.

  """
  @spec create_log_ingest_table_statement(table :: String.t(), opts :: Keyword.t()) :: String.t()
  def create_log_ingest_table_statement(table, opts \\ [])
      when is_non_empty_binary(table) and is_list(opts) do
    database = Keyword.get(opts, :database, nil)

    db_table_string =
      if is_non_empty_binary(database) do
        "#{database}.#{table}"
      else
        "#{table}"
      end

    """
    CREATE TABLE IF NOT EXISTS #{db_table_string} (
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
