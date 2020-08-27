defmodule Logflare.Lql.Encoder do
  @moduledoc """
  Encodes Lql rules to a lql querystring
  """
  alias Logflare.Lql.{Parser, FilterRule, ChartRule}

  def to_querystring(lql_rules) when is_list(lql_rules) do
    lql_rules
    |> Enum.group_by(fn
      %ChartRule{} -> :chart
      %FilterRule{} = f -> f.path
    end)
    |> Enum.sort_by(fn
      {:chart, _} -> 1
      {_path, _} -> 0
    end)
    |> Enum.reduce("", fn
      grouped_rules, qs ->
        append =
          case grouped_rules do
            {:chart, chart_rules} ->
              chart_rules
              |> Enum.map(&to_fragment/1)
              |> Enum.join(" ")
              |> String.replace("chart:timestamp", "")

            {"m.level", filter_rules} when length(filter_rules) >= 2 ->
              {min_level, max_level} =
                Enum.min_max_by(filter_rules, &Parser.Helpers.get_level_order(&1.value))

              "m.level:#{min_level.value}..#{max_level.value}"

            {_path, filter_rules} ->
              filter_rules |> Enum.map(&to_fragment/1) |> Enum.join(" ")
          end

        qs <> " " <> append
    end)
    |> String.trim()
  end

  defp to_fragment(%FilterRule{shorthand: sh} = f) when not is_nil(sh) do
    "#{f.path}:#{sh}" |> String.trim_trailing("s") |> String.replace("timestamp:", "t:")
  end

  defp to_fragment(%FilterRule{modifiers: %{negate: true} = mods} = f) do
    fragment =
      %{f | modifiers: Map.delete(mods, :negate)}
      |> to_fragment()

    "-" <> fragment
  end

  defp to_fragment(%FilterRule{
         path: "timestamp",
         operator: :range,
         values: [lv, rv],
         modifiers: _mods
       }) do
    dtstring =
      if match?(%Date{}, lv) do
        to_datetime_with_range(lv, rv)
        # "#{lv}..#{rv}"
      else
        to_datetime_with_range(lv, rv)
      end

    "t:#{dtstring}"
  end

  defp to_fragment(%FilterRule{path: "timestamp", operator: op, value: v}) do
    dtstring =
      if match?(%Date{}, v) do
        "#{v}"
      else
        v
        |> DateTime.from_naive!("Etc/UTC")
        |> Timex.format!("{ISO:Extended:Z}")
        |> String.trim_trailing("Z")
      end

    "t:#{op}#{dtstring}"
  end

  defp to_fragment(%FilterRule{path: "event_message", value: v, modifiers: mods, operator: op}) do
    op =
      case op do
        :string_contains -> ""
        :"~" -> "~"
      end

    v =
      if mods[:quoted_string] do
        ~s|"#{v}"|
      else
        ~s|#{v}|
      end

    op <> v
  end

  defp to_fragment(%FilterRule{path: path, operator: op, value: v, modifiers: mods} = fr) do
    v =
      if mods[:quoted_string] do
        ~s|"#{v}"|
      else
        v
      end

    case op do
      :range ->
        "#{path}:#{Enum.join(fr.values, "..")}"

      := ->
        "#{path}:#{v}"

      :list_includes ->
        "#{path}:@>#{v}"

      _ ->
        "#{path}:#{op}#{v}"
    end
    |> String.replace_leading("timestamp:", "t:")
    |> String.replace_leading("metadata.", "m.")
  end

  defp to_fragment(%ChartRule{} = c) do
    path =
      case c.path do
        "timestamp" -> "*"
        x -> x
      end

    qs = "c:#{c.aggregate}(#{path}) c:group_by(t::#{c.period})"
    Regex.replace(~r/(?<=sum|avg|count|max|p50|p95|p99)\(metadata./, qs, "(m.")
  end

  def to_datetime_with_range(%Date{} = ldt, %Date{} = rdt) do
    date_periods = [:year, :month, :day]

    mapper = fn period -> timestamp_mapper(ldt, rdt, period) end
    date_periods |> Enum.map(mapper) |> Enum.join("-")
  end

  def to_datetime_with_range(ldt, rdt) do
    date_periods = [:year, :month, :day]
    time_periods = [:hour, :minute, :second]
    us = [:microsecond]

    mapper = fn period -> timestamp_mapper(ldt, rdt, period) end

    date_string = date_periods |> Enum.map(mapper) |> Enum.join("-")
    time_string = time_periods |> Enum.map(mapper) |> Enum.join(":")
    maybe_us_string = us |> Enum.map(mapper) |> hd
    datetime_string = date_string <> "T" <> time_string

    if maybe_us_string != "" do
      datetime_string <> "." <> maybe_us_string
    else
      datetime_string
    end
  end

  defp timestamp_mapper(ldt, rdt, period) do
    {lv, rv} =
      if period != :microsecond do
        lv = Map.get(ldt, period)
        rv = Map.get(rdt, period)

        lv = String.pad_leading("#{lv}", 2, "0")
        rv = String.pad_leading("#{rv}", 2, "0")
        {lv, rv}
      else
        {lv, _} = Map.get(ldt, period)
        {rv, _} = Map.get(rdt, period)

        if lv == 0 and rv == 0 do
          {"", ""}
        else
          lv = String.pad_leading("#{lv}", 6, "0")
          rv = String.pad_leading("#{rv}", 6, "0")
          {lv, rv}
        end
      end

    if lv == rv do
      "#{lv}"
    else
      "{#{lv}..#{rv}}"
    end
  end
end
