defmodule Logflare.Utils.Guards do
  @moduledoc """
  Custom guard functions.

  Utilize these by adding `import Logflare.Utils.Guards` in your module.
  """

  @doc """
  Guard that indicates if the value provided is a number and greater than or equal to 0.
  """
  defguard is_pos_number(num) when is_number(num) and num >= 0

  @doc """
  Guard that indicates if the value provided is an integer and greater than or equal to 0.
  """
  defguard is_non_negative_integer(num) when is_integer(num) and num >= 0

  @doc """
  Guard that indicates if the value provided is an integer and greater than 0.
  """
  defguard is_pos_integer(num) when is_integer(num) and num > 0

  @doc """
  Guard that indicates if the value provided is a binary and not equal to `""`.
  """
  defguard is_non_empty_binary(value) when is_binary(value) and value != ""

  @doc """
  Checks to see if the value is an `atom`, but _not_ a boolean or nil value.
  """
  defguard is_atom_value(value)
           when is_atom(value) and not is_boolean(value) and not is_nil(value)

  @doc """
  Guard that indicates if the value is a `Date` struct.
  """
  defguard is_date(value) when is_struct(value, Date)

  @doc """
  Guard that indicates if the value is a `DateTime` struct.
  """
  defguard is_datetime(value) when is_struct(value, DateTime)

  @doc """
  Guard that indicates if the value is a `DateTime` struct with second precision.
  """
  defguard is_second_precision(value) when is_datetime(value) and value.microsecond == {0, 0}

  @doc """
  Guard that indicates if the value is a `NaiveDateTime` struct.
  """
  defguard is_naive_datetime(value) when is_struct(value, NaiveDateTime)

  @doc """
  Guard that indicates if the value is a valid percentile aggregate.
  """
  defguard is_percentile_aggregate(value) when value in [:p50, :p95, :p99]

  @doc """
  Guard that indicates if the value is a list or map.
  """
  defguard is_list_or_map(value) when is_list(value) or is_map(value)

  @doc """
  Guard that indicates if the value is a valid event type.
  """
  defguard is_event_type(value) when is_atom_value(value) and value in [:log, :metric, :trace]
end
