defmodule Logflare.EctoBigQueryFunctions do
  @moduledoc """
  Custom Ecto functions for BigQuery
  """

  defmacro partition_date do
    quote do
      fragment("_PARTITIONDATE")
    end
  end

  defmacro in_streaming_buffer do
    quote do
      fragment("_PARTITIONDATE IS NULL")
    end
  end

  defmacro bq_current_date() do
    quote do
      fragment("_PARTITIONDATE IS NULL")
    end
  end

  defmacro bq_timestamp_trunc(timestamp, interval) do
    fragment_string = "TIMESTAMP_TRUNC(?, #{to_bq_interval_token(interval)})"

    quote do
      fragment(
        unquote(fragment_string),
        unquote(timestamp)
      )
    end
  end

  defmacro bq_ago(datetime, count, interval) do
    fragment_string = "DATETIME_SUB(?, INTERVAL ? #{to_bq_interval_token(interval)})"

    quote do
      fragment(
        unquote(fragment_string),
        unquote(datetime),
        unquote(count)
      )
    end
  end

  defmacro bq_from_now(datetime, count, interval) do
    fragment_string = "DATETIME_ADD(?, INTERVAL ? #{to_bq_interval_token(interval)})"

    quote do
      fragment(
        unquote(fragment_string),
        unquote(datetime),
        unquote(count)
      )
    end
  end

  defmacro bq_date_add(date, count, interval) do
    fragment_string = "DATE_ADD(?, INTERVAL ? #{to_bq_interval_token(interval)})"

    quote do
      fragment(unquote(fragment_string), unquote(date), unquote(count))
    end
  end

  defmacro bq_date_sub(date, count, interval) do
    fragment_string = "DATE_SUB(?, INTERVAL ? #{to_bq_interval_token(interval)})"

    quote do
      fragment(unquote(fragment_string), unquote(date), unquote(count))
    end
  end

  defp to_bq_interval_token(interval) do
    case interval do
      i when i in ~w(second SECOND) -> "SECOND"
      i when i in ~w(minute MINUTE) -> "MINUTE"
      i when i in ~w(hour HOUR) -> "HOUR"
      i when i in ~w(day DAY) -> "DAY"
      i when i in ~w(week WEEK) -> "WEEK"
      i when i in ~w(month MONTH) -> "MONTH"
    end
  end
end
