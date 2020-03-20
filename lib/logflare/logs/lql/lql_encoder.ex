defmodule Logflare.Lql.Encoder do
  @moduledoc """
  Encodes Lql rules to a lql querystring
  """
  alias Logflare.Lql.{Parser, Encoder, FilterRule, ChartRule}

  def to_querystring(lql_rules) when is_list(lql_rules) do
    lql_rules
    |> Enum.sort_by(fn r ->
      case r do
        %FilterRule{} -> 0
        %ChartRule{} -> 1
      end
    end)
    |> Enum.group_by(fn
      %ChartRule{} -> :chart
      %FilterRule{} = f -> f.path
    end)
    |> Enum.reduce("", fn
      {:chart, chart_rules}, qs ->
        chart_qs =
          chart_rules
          |> Enum.map(&to_fragment/1)
          |> Enum.join(" ")
          |> String.replace("chart:timestamp", "")

        qs <> " " <> chart_qs

      {"metadata.level", filter_rules}, qs when length(filter_rules) >= 2 ->
        {min_level, max_level} =
          Enum.min_max_by(filter_rules, &Parser.Helpers.get_level_order(&1.value))

        qs <> " " <> "metadata.level:#{min_level.value}..#{max_level.value}"

      {path, filter_rules}, qs ->
        qs <> " " <> (Enum.map(filter_rules, &to_fragment/1) |> Enum.join(" "))
    end)
    |> String.trim()
  end

  defp to_fragment(%FilterRule{shorthand: sh} = f) when not is_nil(sh) do
    "#{f.path}:#{sh}" |> String.trim_trailing("s")
  end

  defp to_fragment(%FilterRule{modifiers: %{negate: true} = mods} = f) do
    "-" <> to_fragment(%{f | modifiers: Map.delete(mods, :negate)})
  end

  defp to_fragment(%FilterRule{
         path: "timestamp",
         operator: :range,
         values: [lv, rv],
         modifiers: mods
       }) do
    dtstring =
      if match?(%Date{}, lv) do
        "#{lv}..#{rv}"
      else
        Timex.format!(lv, "{ISO:Extended:Z}") <> ".." <> Timex.format!(rv, "{ISO:Extended:Z}")
      end

    "timestamp:#{dtstring}"
  end

  defp to_fragment(%FilterRule{path: "timestamp", operator: op, value: v, modifiers: mods}) do
    dtstring =
      if match?(%Date{}, v) do
        "#{v}"
      else
        Timex.format!(v, "{ISO:Extended:Z}")
      end

    "timestamp:#{op}#{dtstring}"
  end

  defp to_fragment(%FilterRule{path: "event_message", operator: op, value: v, modifiers: mods}) do
    if mods[:quoted_string] do
      ~s|"#{v}"|
    else
      ~s|#{v}|
    end
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
  end

  defp to_fragment(%ChartRule{} = c) do
    fr = "chart:#{c.path}"

    fr =
      if c.aggregate do
        fr <> " chart:aggregate@#{c.aggregate}"
      else
        fr
      end

    fr =
      if c.aggregate do
        fr <> " chart:period@#{c.period}"
      else
        fr
      end

    fr
  end
end
