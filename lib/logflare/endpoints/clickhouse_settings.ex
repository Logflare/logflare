defmodule Logflare.Endpoints.ClickHouseSettings do
  @moduledoc """
  Typed ClickHouse resource and scheduling policy. Consumer SQL is validated separately;
  trusted settings are combined before being applied to the final assembled query.

  Only internal administrators can configure these limits; the public endpoint
  editor and API do not accept them. The final outer SETTINGS clause is built
  from parsed SQL, and a matching key anywhere in the query is rejected rather
  than relying on ClickHouse precedence between nested/outer settings. For
  hard cluster-wide ceilings, also use ClickHouse query-user profile constraints.
  """

  alias Logflare.Sql.Parser

  @resource_limits ~w(max_bytes_to_read max_rows_to_read max_memory_usage max_execution_time max_threads)
  @setting_types %{
    "priority" => :integer,
    "max_threads" => :integer,
    "max_bytes_to_read" => :integer,
    "max_rows_to_read" => :integer,
    "max_memory_usage" => :integer,
    "max_execution_time" => :float,
    "read_overflow_mode" => {:enum, ["throw"]},
    "timeout_overflow_mode" => {:enum, ["throw"]}
  }
  @max_limit 9_223_372_036_854_775_807

  @spec normalize(map()) :: {:ok, map()} | {:error, String.t()}
  def normalize(settings) when is_map(settings) do
    with :ok <- validate_entries(settings) do
      settings =
        if Enum.any?(["max_bytes_to_read", "max_rows_to_read"], &Map.has_key?(settings, &1)) do
          Map.put(settings, "read_overflow_mode", "throw")
        else
          settings
        end

      settings =
        if Map.has_key?(settings, "max_execution_time"),
          do: Map.put(settings, "timeout_overflow_mode", "throw"),
          else: settings

      {:ok, settings}
    end
  end

  def normalize(_), do: {:error, "Enforced ClickHouse settings must be a map"}

  @spec merge([map()]) :: {:ok, map()} | {:error, String.t()}
  def merge(policies) do
    Enum.reduce_while(policies, {:ok, %{}}, fn policy, {:ok, combined} ->
      case normalize(policy) do
        {:ok, policy} ->
          {:cont, {:ok, Map.merge(combined, policy, &merge_value/3)}}

        error ->
          {:halt, error}
      end
    end)
  end

  @spec merge_value(String.t(), term(), term()) :: term()
  defp merge_value(key, existing, incoming) when key in @resource_limits,
    do: min(existing, incoming)

  defp merge_value(_key, _existing, incoming), do: incoming

  @spec enforce(String.t(), map()) :: {:ok, String.t()} | {:error, String.t()}
  def enforce(query, settings) when settings == %{}, do: {:ok, query}

  def enforce(query, settings) when is_binary(query) and is_map(settings) do
    with {:ok, settings} <- normalize(settings),
         {:ok, [statement]} <- Parser.parse("clickhouse", query),
         :ok <- reject_conflicts(statement, Map.keys(settings)),
         {:ok, [%{"Query" => %{"settings" => setting_nodes}}]} <-
           Parser.parse("clickhouse", settings_sql(settings)),
         {:ok, statement} <- append_settings(statement, setting_nodes),
         {:ok, sql} <- Parser.to_string([statement]) do
      {:ok, sql}
    else
      {:ok, _} -> {:error, "Expected one ClickHouse SELECT or EXPLAIN SELECT query"}
      error -> error
    end
  end

  @spec append_settings(map(), [map()]) :: {:ok, map()} | {:error, String.t()}
  defp append_settings(%{"Query" => ast} = statement, setting_nodes) do
    {:ok, %{statement | "Query" => Map.update!(ast, "settings", &((&1 || []) ++ setting_nodes))}}
  end

  defp append_settings(%{"Explain" => %{"statement" => inner}} = statement, setting_nodes) do
    with {:ok, inner} <- append_settings(inner, setting_nodes) do
      {:ok, put_in(statement, ["Explain", "statement"], inner)}
    end
  end

  defp append_settings(_statement, _setting_nodes),
    do: {:error, "Expected one ClickHouse SELECT or EXPLAIN SELECT query"}

  defp validate_entries(settings) do
    Enum.reduce_while(settings, :ok, fn {key, value}, :ok ->
      if valid_value?(Map.get(@setting_types, key), value) do
        {:cont, :ok}
      else
        {:halt, {:error, "Invalid enforced ClickHouse setting #{inspect(key)} or value"}}
      end
    end)
  end

  @spec valid_value?(term(), term()) :: boolean()
  defp valid_value?(:integer, value),
    do: is_integer(value) and value > 0 and value <= @max_limit

  defp valid_value?(:float, value),
    do: is_number(value) and value > 0 and value <= @max_limit

  defp valid_value?({:enum, values}, value), do: value in values
  defp valid_value?(_type, _value), do: false

  defp settings_sql(settings) do
    clauses =
      settings
      |> Enum.sort()
      |> Enum.map_join(", ", fn
        {key, value} when is_binary(value) -> "#{key} = '#{value}'"
        {key, value} -> "#{key} = #{value}"
      end)

    "SELECT 1 FROM t SETTINGS " <> clauses
  end

  defp reject_conflicts(ast, keys) do
    case conflicting_setting(ast, keys) do
      nil -> :ok
      key -> {:error, "ClickHouse setting #{key} is enforced by query policy"}
    end
  end

  defp conflicting_setting(%{"settings" => settings} = ast, keys) when is_list(settings) do
    Enum.find_value(settings, fn %{"key" => %{"value" => key}} ->
      if String.downcase(key) in keys, do: key
    end) || conflicting_setting(Map.delete(ast, "settings"), keys)
  end

  defp conflicting_setting(ast, keys) when is_map(ast) do
    Enum.find_value(ast, fn {_key, value} -> conflicting_setting(value, keys) end)
  end

  defp conflicting_setting(ast, keys) when is_list(ast) do
    Enum.find_value(ast, &conflicting_setting(&1, keys))
  end

  defp conflicting_setting(_, _), do: nil
end
