defmodule Logflare.Endpoints.ClickHouseSettings do
  @moduledoc """
  Endpoint-owned ClickHouse resource limits. Consumer SQL is validated separately;
  these settings are applied only after the final sandboxed query is assembled.

  Only internal administrators can configure these limits; the public endpoint
  editor and API do not accept them. The final outer SETTINGS clause is built
  from parsed SQL, and a matching key anywhere in the query is rejected rather
  than relying on ClickHouse precedence between nested/outer settings. For
  hard cluster-wide ceilings, also use ClickHouse query-user profile constraints.
  """

  alias Logflare.Sql.Parser

  @resource_limits ~w(max_bytes_to_read max_rows_to_read max_memory_usage max_execution_time)
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

      {:ok, settings}
    end
  end

  def normalize(_), do: {:error, "Enforced ClickHouse settings must be a map"}

  @spec enforce(String.t(), map()) :: {:ok, String.t()} | {:error, String.t()}
  def enforce(query, settings) when settings == %{}, do: {:ok, query}

  def enforce(query, settings) when is_binary(query) and is_map(settings) do
    with {:ok, settings} <- normalize(settings),
         {:ok, [%{"Query" => ast} = statement]} <- Parser.parse("clickhouse", query),
         :ok <- reject_conflicts(statement, Map.keys(settings)),
         {:ok, [%{"Query" => %{"settings" => setting_nodes}}]} <-
           Parser.parse("clickhouse", settings_sql(settings)),
         {:ok, sql} <-
           Parser.to_string([
             %{"Query" => Map.update!(ast, "settings", &((&1 || []) ++ setting_nodes))}
           ]) do
      {:ok, sql}
    else
      {:ok, _} -> {:error, "Expected one ClickHouse SELECT query"}
      error -> error
    end
  end

  defp validate_entries(settings) do
    Enum.reduce_while(settings, :ok, fn
      {key, value}, :ok
      when key in @resource_limits and is_integer(value) and value > 0 and
             value <= @max_limit ->
        {:cont, :ok}

      {"read_overflow_mode", "throw"}, :ok ->
        {:cont, :ok}

      {key, _value}, :ok ->
        {:halt, {:error, "Invalid enforced ClickHouse setting #{inspect(key)} or value"}}
    end)
  end

  defp settings_sql(settings) do
    clauses =
      settings
      |> Enum.sort()
      |> Enum.map_join(", ", fn
        {"read_overflow_mode", "throw"} -> "read_overflow_mode = 'throw'"
        {key, value} -> "#{key} = #{value}"
      end)

    "SELECT 1 FROM t SETTINGS " <> clauses
  end

  defp reject_conflicts(ast, keys) do
    case conflicting_setting(ast, keys) do
      nil -> :ok
      key -> {:error, "ClickHouse setting #{key} is enforced by this endpoint"}
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
