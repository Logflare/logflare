defmodule Logflare.Lql do
  @moduledoc false
  alias Logflare.Source.BigQuery.SchemaBuilder
  alias __MODULE__.{Parser, Encoder}

  @deprecated "Delete when all source rules are migrated to LQL"
  def build_message_filter_from_regex(regex) when is_binary(regex) do
    __MODULE__.Parser.parse(regex, SchemaBuilder.initial_table_schema())
  end

  def encode(lql_rules) do
    {:ok, Encoder.to_querystring(lql_rules)}
  end

  def encode!(lql_rules) do
    Encoder.to_querystring(lql_rules)
  end

end
