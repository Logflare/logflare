defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor.QueryTemplates do
  @moduledoc """
  Common query templates utilized by the `ClickhouseAdaptor`.
  """

  import Logflare.Utils.Guards

  @default_table_engine Application.compile_env(:logflare, :clickhouse_backend_adaptor)[:engine]
  @default_ttl_days 3

  @doc """
  Default naming prefix for the log ingest table.
  """
  def default_table_name_prefix, do: "log_events"

  @doc """
  Default naming prefix for the key type count materialized view.
  """
  def default_key_type_counts_view_prefix, do: "mv_key_type_counts_per_min"

  @doc """
  Default naming prefix for the key type count table.
  """
  def default_key_type_counts_table_prefix, do: "key_type_counts_per_min"

  @doc """
  Generates a ClickHouse query statement to check that the user GRANTs include the needed permissions.

  The results will return a `1` if the user _has_ the needed GRANTs or a `0` otherwise.

  Because this is generally run via a connection that was provided with the
  user credentials and database, there is no need to supply the specific DB by default.

  ###Options

  - `:database` - (Optional) Will produce a fully qualified `<database>.*` string when provided with a value. Defaults to `nil`.

  """
  @spec grant_check_statement(opts :: Keyword.t()) :: String.t()
  def grant_check_statement(opts \\ []) when is_list(opts) do
    database = Keyword.get(opts, :database, nil)

    grant_target_string =
      if is_non_empty_binary(database) do
        "#{database}.*"
      else
        "*"
      end

    "CHECK GRANT CREATE TABLE, ALTER TABLE, INSERT, SELECT, DROP TABLE, CREATE VIEW, DROP VIEW ON #{grant_target_string}"
  end

  @doc """
  Generates a ClickHouse query statement to provision an ingest table for logs.

  ###Options

  - `:database` - (Optional) Will produce a fully qualified `<database>.<table>` string when provided with a value. Defaults to `nil`.
  - `:engine` - (Optional) ClickHouse table engine. Defaults to `"MergeTree"`. Default can be adjusted in `/config/*.exs`.
  - `:ttl_days` - (Optional) Will add a TTL statement to the table creation query. Defaults to `3`. `nil` will disable the TTL.

  """
  @spec create_log_ingest_table_statement(table :: String.t(), opts :: Keyword.t()) :: String.t()
  def create_log_ingest_table_statement(table, opts \\ [])
      when is_non_empty_binary(table) and is_list(opts) do
    database = Keyword.get(opts, :database)
    engine = Keyword.get(opts, :engine, @default_table_engine)
    ttl_days_temp = Keyword.get(opts, :ttl_days, @default_ttl_days)

    ttl_days =
      if is_pos_integer(ttl_days_temp) do
        ttl_days_temp
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
      ENGINE = #{engine}
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
  - `:table` - (Optional) Defaults to `"key_type_counts_per_minute"` and should stay this way, but if you really want to change it - this is the way.
  - `:engine` - (Optional) ClickHouse table engine. Defaults to `"MergeTree"`. Default can be adjusted in `/config/*.exs`.

  """
  @spec create_key_type_counts_table_statement(opts :: Keyword.t()) :: String.t()
  def create_key_type_counts_table_statement(opts \\ []) when is_list(opts) do
    table = Keyword.get(opts, :table)
    database = Keyword.get(opts, :database)
    engine = Keyword.get(opts, :engine, @default_table_engine)

    default_key_count_table_name = default_key_type_counts_table_prefix()

    db_table_string =
      cond do
        is_non_empty_binary(database) and is_non_empty_binary(table) ->
          "#{database}.#{table}"

        is_non_empty_binary(database) ->
          "#{database}.#{default_key_count_table_name}"

        is_non_empty_binary(table) ->
          table

        true ->
          default_key_count_table_name
      end

    """
    CREATE TABLE IF NOT EXISTS #{db_table_string} (
      `minute` DateTime,
      `key` String,
      `type` String,
      `key_count` UInt64
    )
    ENGINE = #{engine}
    PARTITION BY toYYYYMMDD(minute)
    ORDER BY (minute, key, type)
    SETTINGS index_granularity = 8192
    """
  end

  @doc """
  Generates a ClickHouse query statement to create a materialized view, linking the key types data to the source log table.

  ###Options

  - `:database` - (Optional) Will produce a fully qualified `<database>.<table>` string when provided with a value. Defaults to `nil`.
  - `:view_name` - (Optional) Allows overriding the materialized view name. Defaults to `"mv_key_type_counts_per_minute"`.
  - `:key_table` - (Optional) Allows overriding the referenced key table in the mat view. Defaults to `"key_type_counts_per_minute"`.

  """
  @spec create_materialized_view_statement(source_table :: String.t(), opts :: Keyword.t()) ::
          String.t()
  def create_materialized_view_statement(source_table, opts \\ [])
      when is_non_empty_binary(source_table) and is_list(opts) do
    database = Keyword.get(opts, :database)
    view_name = Keyword.get(opts, :view_name)
    key_table = Keyword.get(opts, :key_table)

    default_key_count_view_name = default_key_type_counts_view_prefix()
    default_key_count_table_name = default_key_type_counts_table_prefix()

    db_view_name_string =
      cond do
        is_non_empty_binary(database) and is_non_empty_binary(view_name) ->
          "#{database}.#{view_name}"

        is_non_empty_binary(database) ->
          "#{database}.#{default_key_count_view_name}"

        is_non_empty_binary(view_name) ->
          view_name

        true ->
          default_key_count_view_name
      end

    db_key_table_string =
      cond do
        is_non_empty_binary(database) and is_non_empty_binary(key_table) ->
          "#{database}.#{key_table}"

        is_non_empty_binary(database) ->
          "#{database}.#{default_key_count_table_name}"

        is_non_empty_binary(key_table) ->
          key_table

        true ->
          default_key_count_table_name
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
