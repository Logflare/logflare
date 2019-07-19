defmodule LogflareWeb.Source.TailSearchLV.Utils do
  @moduledoc """
  Various utility functions for tail search liveviews
  """
  alias Logflare.{Sources, Logs}
  alias Logflare.BigQuery.SchemaTypes
  use Phoenix.HTML

  def format_sql({sql, params}) do
    Enum.reduce(params, sql, fn param, sql ->
      type = param.parameterType.type
      value = param.parameterValue.value

      case type do
        "STRING" ->
          String.replace(sql, "?", "'#{value}'", global: false)

        num when num in ~w(INTEGER FLOAT) ->
          String.replace(sql, "?", inspect(value), global: false)

        _ ->
          String.replace(sql, "?", inspect(value), global: false)
      end
    end)
  end

  def format_bq_schema(source) do
    fields_and_types =
      source
      |> Sources.Cache.get_bq_schema()
      |> Logs.Validators.BigQuerySchemaChange.to_typemap()
      |> Iteraptor.to_flatmap()
      |> Enum.reject(fn {_, v} -> v == :map end)
      |> Enum.map(fn {k, v} -> {String.replace(k, ".fields", ""), v} end)
      |> Enum.map(fn {k, v} -> {String.trim_trailing(k, ".t"), v} end)
      |> Enum.map(fn {k, v} -> {k, SchemaTypes.to_schema_type(v)} end)

    ~E"""
    <%= for {field, type} <- fields_and_types do %>
      <ul class="list-group">
      <li class="list-group-item"><%= field %>:<%= type %></li>
      </ul>
    <% end %>
    """
  end
end
