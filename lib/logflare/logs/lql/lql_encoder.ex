defmodule Logflare.Lql.Encoder do
  @moduledoc """
  Encodes Lql rules to a lql querystring
  """
  alias Logflare.Lql.{Parser, Encoder, FilterRule, ChartRule}

  def to_querystring(lql_rules) when is_list(lql_rules) do
    lql_rules
    |> Enum.sort_by(fn
      %FilterRule{} -> 0
      %ChartRule{} -> 1
    end)
    |> Enum.group_by(fn
      %ChartRule{} -> :chart
      %FilterRule{} = f -> f.path
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

            {"metadata.level", filter_rules} when length(filter_rules) >= 2 ->
              {min_level, max_level} =
                Enum.min_max_by(filter_rules, &Parser.Helpers.get_level_order(&1.value))

              "metadata.level:#{min_level.value}..#{max_level.value}"

            {_path, filter_rules} ->
              filter_rules |> Enum.map(&to_fragment/1) |> Enum.join(" ")
          end

        qs <> " " <> append
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
         modifiers: _mods
       }) do
    dtstring =
      if match?(%Date{}, lv) do
        "#{lv}..#{rv}"
      else
        Timex.format!(lv, "{ISO:Extended:Z}") <> ".." <> Timex.format!(rv, "{ISO:Extended:Z}")
      end

    "timestamp:#{dtstring}"
  end

  defp to_fragment(%FilterRule{path: "timestamp", operator: op, value: v}) do
    dtstring =
      if match?(%Date{}, v) do
        "#{v}"
      else
        Timex.format!(v, "{ISO:Extended:Z}")
      end

    "timestamp:#{op}#{dtstring}"
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
  end

  defp to_fragment(%ChartRule{} = c) do
    path =
      case c.path do
        "timestamp" -> "*"
        x -> x
      end

    "chart:#{c.aggregate}(#{path}) chart:group_by(timestamp::#{c.period})"
  end
end
