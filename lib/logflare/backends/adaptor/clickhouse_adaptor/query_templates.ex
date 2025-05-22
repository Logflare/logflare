defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor.QueryTemplates do
  @moduledoc """
  Common query templates utilized by the `ClickhouseAdaptor`.
  """

  import Logflare.Guards

  @default_ttl_days 90

  @doc """
  Generates a ClickHouse query statement to provision an ingest table for logs.

  ###Options

  - `:database` - (Optional) Will produce a fully qualified `<database>.<table>` string when provided with a value. Defaults to `nil`.
  - `:ttl_days` - (Optional) Will add a TTL statement to the table creation query. Defaults to `90`. `nil` will disable the TTL.

  """
  @spec create_log_ingest_table_statement(table :: String.t(), opts :: Keyword.t()) :: String.t()
  def create_log_ingest_table_statement(table, opts \\ [])
      when is_non_empty_binary(table) and is_list(opts) do
    database = Keyword.get(opts, :database, nil)
    ttl_days_temp = Keyword.get(opts, :ttl_days, @default_ttl_days)

    ttl_days =
      if is_pos_integer(ttl_days_temp) do
        ttl_days_temp
      else
        nil
      end

    db_table_string =
      if is_non_empty_binary(database) do
        "#{database}.#{table}"
      else
        "#{table}"
      end

    Enum.join([
      """
      CREATE TABLE IF NOT EXISTS #{db_table_string} (
        "id" UUID,
        "event_message" String,
        "body" String,
        "timestamp" DateTime64(6)
      )
      ENGINE MergeTree()
      PARTITION BY toYYYYMMDD("timestamp")
      ORDER BY ("timestamp")
      """,
      if is_pos_integer(ttl_days) do
        """
        TTL toDateTime("timestamp") + INTERVAL #{ttl_days} DAY
        """
      end,
      "SETTINGS index_granularity = 8192 SETTINGS flatten_nested=0"
    ])
    |> String.trim_trailing("\n")
  end
end
