defmodule Logflare.Lql.Encoder do
  @moduledoc """
  Encodes LQL rules to a LQL querystring
  """

  import Logflare.Utils.Guards

  alias Logflare.Lql.Parser.Helpers
  alias Logflare.Lql.Rules
  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Lql.Rules.FromRule
  alias Logflare.Lql.Rules.SelectRule

  @date_periods ~w(year month day)a
  @time_periods ~w(hour minute second)a

  defguardp is_valid_date_or_datetime(value)
            when is_date(value) or is_datetime(value) or is_naive_datetime(value)

  @spec to_querystring(Rules.lql_rules()) :: String.t()
  def to_querystring(lql_rules) when is_list(lql_rules) do
    lql_rules
    |> Enum.group_by(fn
      %FromRule{} -> :from
      %ChartRule{} -> :chart
      %SelectRule{} -> :select
      %FilterRule{} = f -> f.path
    end)
    |> Enum.sort_by(fn
      {:from, _} -> 0
      {:select, _} -> 1
      {:chart, _} -> 3
      {_path, _} -> 2
    end)
    |> Enum.reduce("", fn
      grouped_rules, qs ->
        append =
          case grouped_rules do
            {:from, from_rules} ->
              from_rules
              |> Enum.map_join(" ", &to_fragment/1)

            {:select, select_rules} ->
              select_rules
              |> Enum.map_join(" ", &to_fragment/1)

            {:chart, chart_rules} ->
              chart_rules
              |> Enum.map_join(" ", &to_fragment/1)
              |> String.replace("chart:timestamp", "")

            # `filter_rules` has at least 2 entries
            {"m.level", [_, _ | _] = filter_rules} ->
              {min_level, max_level} =
                Enum.min_max_by(filter_rules, &Helpers.get_level_order(&1.value))

              "m.level:#{min_level.value}..#{max_level.value}"

            {_path, filter_rules} ->
              Enum.map_join(filter_rules, " ", &to_fragment/1)
          end

        qs <> " " <> append
    end)
    |> String.trim()
  end

  @spec to_fragment(Rules.lql_rule()) :: String.t()
  defp to_fragment(%FilterRule{shorthand: sh} = f) when not is_nil(sh) do
    "#{f.path}:#{sh}"
    |> String.trim_trailing("s")
    |> String.replace("timestamp:", "t:")
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
    "t:#{to_datetime_with_range(lv, rv)}"
  end

  defp to_fragment(%FilterRule{path: "timestamp", operator: op, value: %Date{} = v}),
    do: "t:#{op}#{v}"

  defp to_fragment(%FilterRule{path: "timestamp", operator: op, value: v}) do
    dtstring =
      v
      |> DateTime.from_naive!("Etc/UTC")
      |> Timex.format!("{ISO:Extended:Z}")
      |> String.trim_trailing("Z")

    "t:#{op}#{dtstring}"
  end

  defp to_fragment(%FilterRule{
         path: "event_message",
         value: v,
         modifiers: %{quoted_string: true},
         operator: op
       })
       when op in [:string_contains, :=],
       do: ~s|"#{v}"|

  defp to_fragment(%FilterRule{path: "event_message", value: v, operator: op})
       when op in [:string_contains, :=],
       do: v

  defp to_fragment(%FilterRule{
         path: "event_message",
         value: v,
         modifiers: %{quoted_string: true},
         operator: :"~"
       }),
       do: ~s|~"#{v}"|

  defp to_fragment(%FilterRule{path: "event_message", value: v, operator: :"~"}), do: "~#{v}"

  defp to_fragment(%FilterRule{path: path, operator: :range, values: values}) do
    "#{path}:#{Enum.join(values, "..")}"
    |> String.replace_leading("timestamp:", "t:")
    |> String.replace_leading("metadata.", "m.")
  end

  defp to_fragment(%FilterRule{
         path: path,
         operator: :=,
         value: v,
         modifiers: %{quoted_string: true}
       }) do
    ~s|#{path}:"#{v}"|
    |> String.replace_leading("timestamp:", "t:")
    |> String.replace_leading("metadata.", "m.")
  end

  defp to_fragment(%FilterRule{path: path, operator: :=, value: v}) do
    "#{path}:#{v}"
    |> String.replace_leading("timestamp:", "t:")
    |> String.replace_leading("metadata.", "m.")
  end

  defp to_fragment(%FilterRule{
         path: path,
         operator: :list_includes,
         value: v,
         modifiers: %{quoted_string: true}
       }) do
    ~s|#{path}:@>"#{v}"|
    |> String.replace_leading("timestamp:", "t:")
    |> String.replace_leading("metadata.", "m.")
  end

  defp to_fragment(%FilterRule{path: path, operator: :list_includes, value: v}) do
    "#{path}:@>#{v}"
    |> String.replace_leading("timestamp:", "t:")
    |> String.replace_leading("metadata.", "m.")
  end

  defp to_fragment(%FilterRule{
         path: path,
         operator: :list_includes_regexp,
         value: v,
         modifiers: %{quoted_string: true}
       }) do
    ~s|#{path}:@>~"#{v}"|
    |> String.replace_leading("timestamp:", "t:")
    |> String.replace_leading("metadata.", "m.")
  end

  defp to_fragment(%FilterRule{path: path, operator: :list_includes_regexp, value: v}) do
    "#{path}:@>~#{v}"
    |> String.replace_leading("timestamp:", "t:")
    |> String.replace_leading("metadata.", "m.")
  end

  defp to_fragment(%FilterRule{
         path: path,
         operator: op,
         value: v,
         modifiers: %{quoted_string: true}
       }) do
    ~s|#{path}:#{op}"#{v}"|
    |> String.replace_leading("timestamp:", "t:")
    |> String.replace_leading("metadata.", "m.")
  end

  defp to_fragment(%FilterRule{path: path, operator: op, value: v}) do
    "#{path}:#{op}#{v}"
    |> String.replace_leading("timestamp:", "t:")
    |> String.replace_leading("metadata.", "m.")
  end

  defp to_fragment(%ChartRule{path: "timestamp", aggregate: agg, period: period}) do
    qs = "c:#{agg}(*) c:group_by(t::#{period})"
    Regex.replace(~r/(?<=sum|avg|count|max|p50|p95|p99)\(metadata./, qs, "(m.")
  end

  defp to_fragment(%ChartRule{path: path, aggregate: agg, period: period}) do
    qs = "c:#{agg}(#{path}) c:group_by(t::#{period})"
    Regex.replace(~r/(?<=sum|avg|count|max|p50|p95|p99)\(metadata./, qs, "(m.")
  end

  defp to_fragment(%SelectRule{path: "*"}), do: "s:*"

  defp to_fragment(%SelectRule{path: path}),
    do: "s:#{String.replace_leading(path, "metadata.", "m.")}"

  defp to_fragment(%FromRule{table: table}), do: "f:#{table}"

  def to_datetime_with_range(%Date{} = ldt, %Date{} = rdt) do
    mapper_fn = fn period -> timestamp_mapper(ldt, rdt, period) end
    Enum.map_join(@date_periods, "-", mapper_fn)
  end

  def to_datetime_with_range(ldt, rdt)
      when is_valid_date_or_datetime(ldt) and is_valid_date_or_datetime(rdt) do
    mapper_fn = fn period -> timestamp_mapper(ldt, rdt, period) end
    date_string = Enum.map_join(@date_periods, "-", mapper_fn)
    time_string = Enum.map_join(@time_periods, ":", mapper_fn)
    maybe_usec_string = Enum.map([:microsecond], mapper_fn) |> hd()
    datetime_string = date_string <> "T" <> time_string

    if is_non_empty_binary(maybe_usec_string) do
      datetime_string <> "." <> maybe_usec_string
    else
      datetime_string
    end
  end

  defp timestamp_mapper(
         %{microsecond: {0, _}} = _ldt,
         %{microsecond: {0, _}} = _rdt,
         :microsecond
       ) do
    ""
  end

  defp timestamp_mapper(
         %{microsecond: {lv, _}} = _ldt,
         %{microsecond: {rv, _}} = _rdt,
         :microsecond
       ) do
    {lv, rv} =
      [lv, rv]
      |> Enum.map(&to_string/1)
      |> Enum.map(&String.pad_leading(&1, 6, "0"))
      |> Enum.map(&String.trim_trailing(&1, "0"))
      |> Enum.map(&if &1 == "", do: "0", else: &1)
      |> List.to_tuple()

    if lv == rv do
      "#{lv}"
    else
      "{#{lv}..#{rv}}"
    end
  end

  defp timestamp_mapper(ldt, rdt, period) do
    lv = Map.get(ldt, period)
    rv = Map.get(rdt, period)

    lv = String.pad_leading("#{lv}", 2, "0")
    rv = String.pad_leading("#{rv}", 2, "0")

    if lv == rv do
      "#{lv}"
    else
      "{#{lv}..#{rv}}"
    end
  end
end
