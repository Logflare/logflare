defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor.QueryTemplates do
  @moduledoc """
  Common query templates utilized by the `ClickhouseAdaptor`.
  """

  import Logflare.Utils.Guards

  @key_type_counts_view_name "mv_key_type_counts_per_minute"
  @key_type_counts_table_name "key_type_counts_per_minute"
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
        `id` UUID,
        `event_message` String,
        `body` String,
        `timestamp` DateTime64(6)
      )
      ENGINE MergeTree()
      PARTITION BY toYYYYMMDD(timestamp)
      ORDER BY (timestamp)
      """,
      if is_pos_integer(ttl_days) do
        """
        TTL toDateTime(timestamp) + INTERVAL #{ttl_days} DAY
        """
      end,
      "SETTINGS index_granularity = 8192 SETTINGS flatten_nested=0"
    ])
    |> String.trim_trailing("\n")
  end

  @doc """
  Generates a ClickHouse query statement to provision a table for tracking key types over time.

  This currently defaults to a table name of `"key_type_counts_per_minute"`.

  ###Options

  - `:database` - (Optional) Will produce a fully qualified `<database>.<table>` string when provided with a value. Defaults to `nil`.

  """
  @spec create_key_type_counts_table_statement(table :: String.t(), opts :: Keyword.t()) ::
          String.t()
  def create_key_type_counts_table_statement(table \\ @key_type_counts_table_name, opts \\ [])
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
      `minute` DateTime,
      `key` String,
      `type` String,
      `key_count` UInt64
    )
    ENGINE = SharedSummingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
    PARTITION BY toYYYYMMDD(minute)
    ORDER BY (minute, key, type)
    SETTINGS index_granularity = 8192
    """
  end

  @doc """
  Generates a ClickHouse query statement to create a materialized view, linking the key types data to the source log table.

  ###Options

  - `:database` - (Optional) Will produce a fully qualified `<database>.<table>` string when provided with a value. Defaults to `nil`.
  """
  @spec create_materialized_view_statement(source_table :: String.t(), opts :: Keyword.t()) ::
          String.t()
  def create_materialized_view_statement(source_table, opts \\ [])
      when is_non_empty_binary(source_table) and is_list(opts) do
    database = Keyword.get(opts, :database, nil)

    db_view_name_string =
      if is_non_empty_binary(database) do
        "#{database}.#{@key_type_counts_view_name}"
      else
        "#{@key_type_counts_view_name}"
      end

    db_key_table_string =
      if is_non_empty_binary(database) do
        "#{database}.#{@key_type_counts_table_name}"
      else
        "#{@key_type_counts_table_name}"
      end

    db_source_table_string =
      if is_non_empty_binary(database) do
        "#{database}.#{source_table}"
      else
        "#{source_table}"
      end

    """
    CREATE MATERIALIZED VIEW IF NOT EXISTS #{db_view_name_string} TO #{db_key_table_string}
    AS
    SELECT
      toStartOfMinute(timestamp) AS minute,
      key,
      JSONType(body, key) AS type,
      count() AS key_count
    FROM (
      SELECT
        arrayJoin(JSONExtractKeys(body)) AS key,
        body,
        timestamp
      FROM #{db_source_table_string}
    )
    GROUP BY minute, key, type
    """
  end
end
