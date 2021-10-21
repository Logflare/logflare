defmodule Logflare.SourceSchemas.Cache do
  @moduledoc false

  alias Logflare.Source
  alias Logflare.SourceSchemas
  alias Logflare.Google.BigQuery.SchemaUtils

  def child_spec(_) do
    %{id: __MODULE__, start: {Cachex, :start_link, [__MODULE__, [stats: false, limit: 100_000]]}}
  end

  def get_source_schema(id), do: apply_fun(__ENV__.function, [id])
  def get_source_schema_by(kv), do: apply_fun(__ENV__.function, [kv])

  defp apply_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(SourceSchemas, arg1, arg2)
  end
end
