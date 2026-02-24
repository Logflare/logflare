defmodule Logflare.DateTimeUtils do
  @moduledoc "Various DateTime utilities"
  @type dt_or_ndt :: DateTime.t() | NaiveDateTime.t()
  @type granularity :: :day | :hour | :microsecond | :millisecond | :minute | :month | :second

  @spec truncate(dt_or_ndt, granularity) :: dt_or_ndt
  def truncate(datetime, granularity) when is_atom(granularity) do
    do_truncate(datetime, granularity)
  end

  @spec do_truncate(dt_or_ndt, granularity) :: dt_or_ndt
  defp do_truncate(datetime, granularity)

  defp do_truncate(dt, :month) do
    do_truncate(%{dt | day: 1}, :day)
  end

  defp do_truncate(dt, :day) do
    do_truncate(%{dt | hour: 0}, :hour)
  end

  defp do_truncate(dt, :hour) do
    do_truncate(%{dt | minute: 0}, :minute)
  end

  defp do_truncate(dt, :minute) do
    do_truncate(%{dt | second: 0}, :second)
  end

  defp do_truncate(%DateTime{} = dt, gr) when gr in ~w(second millisecond microsecond)a do
    DateTime.truncate(dt, gr)
  end

  defp do_truncate(%NaiveDateTime{} = dt, gr) when gr in ~w(second millisecond microsecond)a do
    NaiveDateTime.truncate(dt, gr)
  end

  def humanize_timezone_offset(offset) do
    {hours, minutes, _, _} =
      offset
      |> Timex.Duration.from_seconds()
      |> Timex.Duration.to_clock()

    hours =
      case "#{hours}" do
        "-" <> rest when abs(hours) < 10 -> "-0" <> rest
        rest when abs(hours) < 10 -> "+0" <> rest
        x when hours >= 10 -> "+" <> x
        x when hours <= 10 -> x
      end

    minutes_prefix =
      if abs(minutes) < 10 do
        "0"
      else
        ""
      end

    minutes = "#{minutes_prefix}#{minutes}"

    "#{hours}:#{minutes}"
  end
end
