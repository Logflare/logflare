defmodule Logflare.EctoBigQueryFunctions do
  @moduledoc """
  Custom Ecto functions for BigQuery
  """

  defmacro in_streaming_buffer do
    quote do
      fragment("_PARTITIONDATE IS NULL")
    end
  end

  defmacro partitiondate_is_nil do
    quote do
      fragment("_PARTITIONDATE IS NULL")
    end
  end

  defmacro partitiondate_from_now(count, interval) do
    case interval do
      i when i in ~w(day DAY) ->
        quote do
          fragment(
            "_PARTITIONDATE >= DATE_ADD(CURRENT_DATE(), INTERVAL ? DAY)",
            unquote(count)
          )
        end

      i when i in ~w(week WEEK) ->
        quote do
          fragment(
            "_PARTITIONDATE >= DATE_ADD(CURRENT_DATE(), INTERVAL ? WEEK)",
            unquote(count)
          )
        end

      i when i in ~w(month MONTH) ->
        quote do
          fragment(
            "_PARTITIONDATE >= DATE_ADD(CURRENT_DATE(), INTERVAL ? MONTH)",
            unquote(count)
          )
        end
    end
  end

  defmacro partitiondate_ago(count, interval) do
    case interval do
      i when i in ~w(day DAY) ->
        quote do
          fragment(
            "_PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL ? DAY)",
            unquote(count)
          )
        end

      i when i in ~w(week WEEK) ->
        quote do
          fragment(
            "_PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL ? WEEK)",
            unquote(count)
          )
        end

      i when i in ~w(month MONTH) ->
        quote do
          fragment(
            "_PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL ? MONTH)",
            unquote(count)
          )
        end
    end
  end
end
