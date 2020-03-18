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
      {:chart, filter_rules}, qs ->
        qs <> " " <> (Enum.map(filter_rules, &to_fragment/1) |> Enum.join(" "))

      {"metadata.level", filter_rules}, qs when length(filter_rules) >= 2 ->
        {min_level, max_level} = Enum.min_max_by(filter_rules, &Parser.Helpers.get_level_order(&1.value))
        qs <> " " <> "metadata.level:#{min_level.value}..#{max_level.value}"

      {path, filter_rules}, qs when length(filter_rules) == 2 ->
        op_set =
          filter_rules
          |> Enum.map(& &1.operator)
          |> MapSet.new()

        range_operator_fragments? = MapSet.subset?(op_set, MapSet.new([:>=, :>, :<=, :<]))

        if range_operator_fragments? do
          left = Enum.find(filter_rules, &(&1.operator in [:>, :>=]))
          right = Enum.find(filter_rules, &(&1.operator in [:<=, :<]))

          left_value = if match?(%DateTime{}, left.value) do
            Timex.format!(left.value, "{ISO:Extended:Z}")
          else
            "#{left.value}"
          end

          right_value = if match?(%DateTime{}, right.value) do
            Timex.format!(right.value, "{ISO:Extended:Z}")
          else
            "#{right.value}"
          end
          qs <> " " <> "#{path}:#{left_value}..#{right_value}"
        else
          qs <> " " <> (Enum.map(filter_rules, &to_fragment/1) |> Enum.join(" "))
        end

      {path, filter_rules}, qs ->
        qs <> " " <> (Enum.map(filter_rules, &to_fragment/1) |> Enum.join(" "))
    end)
    |> String.trim()
  end

  defp to_fragment(%FilterRule{path: "timestamp", operator: op, value: v, modifiers: mods}) do
    dtstring = if match?(%Date{}, v)  do
      "#{v}"
    else
      Timex.format!(v,  "{ISO:Extended:Z}")
    end
    "timestamp:#{op}#{dtstring}"
  end

  defp to_fragment(%FilterRule{path: "event_message", operator: op, value: v, modifiers: mods}) do
    if :quoted_string in mods do
      ~s|"#{v}"|
    else
      ~s|#{v}|
    end
  end

  defp to_fragment(%FilterRule{path: path, operator: op, value: v, modifiers: mods}) do
    v =
      if :quoted_string in mods do
        ~s|"#{v}"|
      else
        v
      end

    lql_string =
      case op do
        := -> "#{path}:#{v}"
        :list_includes -> "#{path}:@>#{v}"
        _ -> "#{path}:#{op}#{v}"
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
