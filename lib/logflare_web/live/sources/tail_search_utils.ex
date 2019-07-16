defmodule LogflareWeb.Source.TailSearchLV.Utils do
  @moduledoc """
  Various utility functions for tail search liveviews
  """
  def format_sql({sql, params}) do
    Enum.reduce(params, sql, fn param, sql ->
      type = param.parameterType.type
      value = param.parameterValue.value

      case type do
        "STRING" ->
          String.replace(sql, "?", "'#{value}'", global: false)

        num when num in ~w(INTEGER FLOAT) ->
          String.replace(sql, "?", value, global: false)

        _ ->
          String.replace(sql, "?", value, global: false)
      end
    end)
  end
end
