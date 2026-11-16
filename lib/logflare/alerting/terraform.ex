defmodule Logflare.Alerting.Terraform do
  @moduledoc """
  Renders an alert as the `logflare_alert` Terraform resource that manages it.

  Alert issue rules are changed through Terraform, so the Logflare UI shows this
  HCL for operators to copy into their configuration.
  """

  alias Logflare.Alerting.AlertQuery

  @attributes [
    :name,
    :description,
    :language,
    :query,
    :cron,
    :rule_key,
    :managed,
    :severity,
    :category,
    :group_by,
    :scope,
    :resolve_after_runs,
    :enabled
  ]
  @signal_mapping_order ~w(identifiers title_column description_column severity_column category_column metadata_columns window_seconds preview notebook)
  @inline_order ~w(title source sql)
  @bare_key ~r/^[A-Za-z_][A-Za-z0-9_]*$/

  @spec to_hcl(AlertQuery.t()) :: String.t()
  def to_hcl(%AlertQuery{} = alert) do
    attributes =
      @attributes
      |> Enum.map(&{Atom.to_string(&1), Map.get(alert, &1)})
      |> Enum.reject(fn {_key, value} -> blank?(value) end)
      |> Kernel.++(signal_mapping(alert.signal_mapping))

    """
    resource "logflare_alert" "#{label(alert)}" {
    #{block(attributes, "  ")}}
    """
  end

  @spec label(AlertQuery.t()) :: String.t()
  def label(%AlertQuery{rule_key: rule_key, name: name}) do
    label =
      (rule_key || name || "")
      |> String.downcase()
      |> String.replace(~r/[^a-z0-9_]/, "_")

    if Regex.match?(~r/^[a-z_]/, label), do: label, else: "alert_" <> label
  end

  defp signal_mapping(mapping) when is_map(mapping) and map_size(mapping) > 0 do
    {known, rest} = Map.split(mapping, @signal_mapping_order)

    entries =
      @signal_mapping_order
      |> Enum.filter(&Map.has_key?(known, &1))
      |> Enum.map(&{&1, known[&1]})
      |> Kernel.++(Enum.sort(rest))

    case Enum.reject(entries, fn {_key, value} -> blank?(value) end) do
      [] -> []
      entries -> [{"signal_mapping", {:block, entries}}]
    end
  end

  defp signal_mapping(_mapping), do: []

  defp blank?(nil), do: true
  defp blank?(""), do: true
  defp blank?([]), do: true
  defp blank?(map) when is_map(map) and map_size(map) == 0, do: true
  defp blank?(_value), do: false

  defp block(attributes, indent) do
    width =
      attributes
      |> Enum.reject(fn {_key, value} -> nested?(value) end)
      |> Enum.map(fn {key, _value} -> String.length(key(key)) end)
      |> Enum.max(fn -> 0 end)

    Enum.map_join(attributes, fn {key, value} ->
      key = key(key)
      padding = if nested?(value), do: "", else: String.duplicate(" ", width - String.length(key))
      "#{indent}#{key}#{padding} = #{value(value, indent)}\n"
    end)
  end

  defp nested?({:block, _entries}), do: true
  defp nested?([%{} | _]), do: true
  defp nested?(_value), do: false

  defp value({:block, entries}, indent), do: "{\n#{block(entries, indent <> "  ")}#{indent}}"

  defp value([%{} | _] = maps, indent) do
    items = Enum.map_join(maps, fn map -> "#{indent}  #{inline(map)},\n" end)
    "[\n#{items}#{indent}]"
  end

  defp value(string, indent) when is_binary(string) do
    if String.contains?(string, "\n"), do: heredoc(string, indent), else: inline(string)
  end

  defp value(value, _indent), do: inline(value)

  defp heredoc(string, indent) do
    lines = String.split(string, "\n")
    marker = if Enum.any?(lines, &(String.trim(&1) == "SQL")), do: "EOT", else: "SQL"
    body = Enum.map_join(lines, fn line -> "#{indent}  #{escape_template(line)}\n" end)
    "<<-#{marker}\n#{body}#{indent}#{marker}"
  end

  defp inline(value) when is_binary(value), do: ~s("#{escape(value)}")
  defp inline(value) when is_boolean(value) or is_number(value), do: to_string(value)
  defp inline(value) when is_atom(value), do: inline(Atom.to_string(value))

  defp inline(values) when is_list(values),
    do: "[" <> Enum.map_join(values, ", ", &inline/1) <> "]"

  defp inline(map) when is_map(map) do
    entries =
      map
      |> Enum.map(fn {key, value} -> {to_string(key), value} end)
      |> Enum.sort_by(fn {key, _value} -> {inline_rank(key), key} end)
      |> Enum.map_join(", ", fn {key, value} -> "#{key(key)} = #{inline(value)}" end)

    "{ #{entries} }"
  end

  defp inline_rank(key), do: Enum.find_index(@inline_order, &(&1 == key)) || length(@inline_order)

  defp key(key) do
    if Regex.match?(@bare_key, key), do: key, else: inline(key)
  end

  defp escape(string) do
    string
    |> String.replace("\\", "\\\\")
    |> String.replace("\"", "\\\"")
    |> String.replace("\n", "\\n")
    |> String.replace("\r", "\\r")
    |> String.replace("\t", "\\t")
    |> escape_template()
  end

  defp escape_template(string) do
    string
    |> String.replace("${", "$${")
    |> String.replace("%{", "%%{")
  end
end
