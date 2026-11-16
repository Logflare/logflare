defmodule Logflare.AlertIssues.Signals do
  @moduledoc """
  Builds alert signals from the result rows of a scheduled alert run.

  Rows are grouped by the alert's `group_by` columns, one firing signal per group.
  Identifiers combine the group values, the alert's `signal_mapping["identifiers"]`
  (identifier key to row column) and the alert's literal `scope`.
  """

  alias Logflare.AlertIssues
  alias Logflare.Alerting.AlertQuery

  @sample_rows 5

  @spec from_rows(AlertQuery.t(), [map()]) :: [AlertIssues.signal()]
  def from_rows(%AlertQuery{} = alert, rows) when is_list(rows) do
    group_by = alert.group_by || []

    rows
    |> Enum.group_by(&group_values(&1, group_by))
    |> Enum.map(fn {values, group_rows} -> to_signal(alert, values, group_rows) end)
  end

  @spec fingerprint(String.t(), map()) :: String.t()
  def fingerprint(rule_key, group_values) when is_map(group_values) do
    payload = [rule_key | group_values |> Enum.sort() |> Enum.map(fn {k, v} -> "#{k}=#{v}" end)]

    :crypto.hash(:sha256, Enum.join(payload, "\n"))
    |> Base.encode16(case: :lower)
    |> binary_part(0, 32)
  end

  defp group_values(row, group_by) do
    Map.new(group_by, fn column -> {column, stringify(Map.get(row, column))} end)
  end

  defp to_signal(alert, values, group_rows) do
    %{
      fingerprint: fingerprint(alert.rule_key, values),
      status: :firing,
      title: title(alert.name, alert.group_by || [], values),
      identifiers: identifiers(alert, values, hd(group_rows)),
      metadata: %{
        "sample_rows" => Enum.take(group_rows, @sample_rows),
        "row_count" => length(group_rows)
      }
    }
  end

  defp title(name, [], _values), do: name

  defp title(name, group_by, values),
    do: "#{name}: " <> Enum.map_join(group_by, ", ", &Map.get(values, &1))

  defp identifiers(alert, values, row) do
    mapped =
      alert.signal_mapping
      |> Kernel.||(%{})
      |> Map.get("identifiers", %{})
      |> Map.new(fn {key, column} -> {key, stringify(Map.get(row, column))} end)

    values
    |> Map.merge(mapped)
    |> Map.merge(Map.new(alert.scope || %{}, fn {k, v} -> {k, stringify(v)} end))
  end

  defp stringify(nil), do: nil
  defp stringify(value) when is_binary(value), do: value
  defp stringify(value), do: to_string(value)
end
