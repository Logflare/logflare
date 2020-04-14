defmodule Logflare.DateTimeUtils do
  @moduledoc "Various DateTime utilities"
  def truncate(datetime, granularity) do
    do_truncate(datetime, granularity)
  end

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

  defp do_truncate(dt, gr) when gr in ~w(second millisecond microsecond)a do
    DateTime.truncate(dt, gr)
  end
end
