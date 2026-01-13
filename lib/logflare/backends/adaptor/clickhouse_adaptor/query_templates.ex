defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryTemplates do
  @moduledoc """
  Common query templates utilized by the `ClickHouseAdaptor`.
  """

  import Logflare.Utils.Guards

  @default_table_engine Application.compile_env(:logflare, :clickhouse_backend_adaptor)[:engine]
  @default_ttl_days 5

  @doc """
  Default naming prefix for ingest tables.
  """
  def default_table_name_prefix, do: "ingest"

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
  Generates a ClickHouse query statement to provision an ingest table.

  ###Options

  - `:database` - (Optional) Will produce a fully qualified `<database>.<table>` string when provided with a value. Defaults to `nil`.
  - `:engine` - (Optional) ClickHouse table engine. Defaults to `"MergeTree"`. Default can be adjusted in `/config/*.exs`.
  - `:ttl_days` - (Optional) Will add a TTL statement to the table creation query. Defaults to `5`. `nil` will disable the TTL.

  """
  @spec create_ingest_table_statement(table :: String.t(), opts :: Keyword.t()) :: String.t()
  def create_ingest_table_statement(table, opts \\ [])
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
        `source_id` UUID,
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
end
