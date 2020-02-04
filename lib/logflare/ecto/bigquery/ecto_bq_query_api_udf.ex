defmodule Logflare.Ecto.BQQueryAPI.UDF do
  @moduledoc """
  UDF Ecto functions for BigQuery
  """
  import Logflare.Ecto.BQQueryAPI, only: [to_bq_interval_token: 1]

  defmacro lf_timestamp_trunc(timestamp, interval) do
    fragment_string = udf_function("LF_TIMESTAMP_TRUNC(?, ?)")

    interval = quoted_interval(interval)

    quote do
      fragment(
        unquote(fragment_string),
        unquote(timestamp),
        ^unquote(quote(do: to_bq_interval_token(unquote(interval))))
      )
    end
  end

  defmacro lf_timestamp_sub(date, count, interval) do
    fragment_string = udf_function("LF_TIMESTAMP_SUB(?, ?, ?)")

    interval = quoted_interval(interval)

    quote do
      fragment(
        unquote(fragment_string),
        unquote(date),
        unquote(count),
        ^unquote(quote(do: to_bq_interval_token(unquote(interval))))
      )
    end
  end

  def quoted_interval(interval) do
    case interval do
      interval when is_binary(interval) or is_atom(interval) -> interval
      {:^, [line: _], [interval_no_hat]} -> interval_no_hat
    end
  end

  def default_dataset_token() do
    "`$$__DEFAULT_DATASET__$$`"
  end

  def udf_function(function_string) do
    "(#{default_dataset_token()}.#{function_string})"
  end
end
