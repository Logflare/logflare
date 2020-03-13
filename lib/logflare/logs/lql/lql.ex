defmodule Logflare.Lql do
  @moduledoc false
  alias Logflare.Source.BigQuery.SchemaBuilder
  @deprecated "Delete when all source rules are migrated to LQL"
  def build_message_filter_from_regex(regex) when is_binary(regex) do
    __MODULE__.Parser.parse(regex, SchemaBuilder.initial_table_schema())
  end
end
