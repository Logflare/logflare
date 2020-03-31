defmodule Logflare.Lql.Encoder do
  @moduledoc """
  Encodes Lql rules to a lql querystring
  """
  alias Logflare.Lql.{Parser, Encoder, FilterRule, ChartRule}

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
        lvstring =
          lv
          |> DateTime.from_naive!("Etc/UTC")
          |> Timex.format!("{ISO:Extended:Z}")
          |> String.trim_trailing("Z")

        rvstring =
          rv
          |> DateTime.from_naive!("Etc/UTC")
          |> Timex.format!("{ISO:Extended:Z}")
          |> String.trim_trailing("Z")

        to_datetime_with_range(lvstring, rvstring)
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
    |> String.replace("timestamp:", "t:")
  end

  defp to_fragment(%ChartRule{} = c) do
    path =
      case c.path do
        "timestamp" -> "*"
        x -> x
      end

    "c:#{c.aggregate}(#{path}) c:group_by(t::#{c.period})"
  end

  def to_datetime_with_range(lvstring, rvstring) do
    myers_diff = String.myers_difference(lvstring, rvstring)

    del_ins_count =
      myers_diff
      |> Enum.filter(&(elem(&1, 0) in ~w[del ins]a))
      |> Enum.count()

    if del_ins_count == 2 do
      original_myers_diff = myers_diff

      {start, myers_diff} = Keyword.pop_first(myers_diff, :eq)
      {l, myers_diff} = Keyword.pop_first(myers_diff, :del)
      {r, myers_diff} = Keyword.pop_first(myers_diff, :ins)
      {maybe_eq, end_} = Keyword.pop_first(myers_diff, :eq)

      {maybe_eq, end_} =
        if end_ == [] do
          {nil, maybe_eq}
        else
          {maybe_eq, end_[:eq]}
        end

      cond do
        maybe_eq ->
          case Keyword.keys(original_myers_diff) |> Enum.reject(&(&1 == :eq)) do
            [:del, :ins] ->
              start <> "{#{l}#{maybe_eq}..#{maybe_eq}#{r}}" <> end_

            [:ins, :del] ->
              start <> "{#{maybe_eq}#{l}..#{r}#{maybe_eq}}" <> end_
          end

        String.first(end_) not in ["T", ":", "-"] ->
          {common, end_} = String.split_at(end_, 1)
          start <> "{#{l}#{common}..#{r}#{common}}" <> end_

        String.last(start) not in ["T", ":", "-"] ->
          {start, common} = String.split_at(start, -1)
          start <> "{#{common}#{l}..#{common}#{r}}" <> end_

        true ->
          start <> "{#{l}..#{r}}" <> end_
      end
    else
      lvstring <> ".." <> rvstring
    end
  end
end
