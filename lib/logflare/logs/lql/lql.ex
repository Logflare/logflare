defmodule Logflare.Lql do
  @moduledoc false
  alias Logflare.Source.BigQuery.SchemaBuilder
  alias GoogleApi.BigQuery.V2.Model.TableSchema, as: TS
  alias __MODULE__.{Parser, Encoder}

  @deprecated "Delete when all source rules are migrated to LQL"
  def build_message_filter_from_regex(regex) when is_binary(regex) do
    __MODULE__.Parser.parse(regex, SchemaBuilder.initial_table_schema())
  end

  def decode(qs, %TS{} = bq_table_schema) when is_binary(qs) do
    Parser.parse(qs, bq_table_schema)
  end

  def decode!(qs, %TS{} = bq_table_schema) when is_binary(qs) do
    {:ok, lql_rules} = decode(qs, bq_table_schema)
    lql_rules
  end

  def encode(lql_rules) when is_list(lql_rules) do
    {:ok, Encoder.to_querystring(lql_rules)}
  end

  def encode!(lql_rules) do
    Encoder.to_querystring(lql_rules)
  end
end
