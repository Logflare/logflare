defmodule Logflare.Endpoints.ClickHouseSqlSettings do
  @moduledoc """
  Literal, outer-query settings that can only tighten an endpoint's inherited policy.
  Resource requests use the smaller limit; priority requests use the larger number.
  Every requested key requires an explicit inherited baseline at execution time.
  """

  require Logger

  alias Logflare.Endpoints.ClickHouseSettings
  alias Logflare.Sql.Parser

  @keys ~w(max_execution_time max_memory_usage max_rows_to_read max_bytes_to_read max_threads priority)

  @spec setting?(String.t()) :: boolean()
  defp setting?(key), do: key in @keys

  @spec split_requests([map()]) :: {:ok, [map()], map()} | {:error, String.t()}
  def split_requests([statement]) do
    with {:ok, statement, requests} <- extract(statement), do: {:ok, [statement], requests}
  end

  def split_requests(_), do: {:error, "Expected one ClickHouse SELECT or EXPLAIN SELECT query"}

  @spec validate_sandboxed([map()], [map()] | nil) :: :ok | {:error, String.t()}
  def validate_sandboxed(_statements, nil), do: :ok

  def validate_sandboxed(statements, sandboxed_ast) do
    case requested_setting([statements, sandboxed_ast]) do
      nil -> :ok
      key -> {:error, "ClickHouse setting #{key} is not supported in sandboxed SQL"}
    end
  end

  @spec enforce(String.t(), map(), keyword()) :: {:ok, String.t()} | {:error, String.t()}
  def enforce(query, inherited, opts \\ []) do
    if Regex.match?(~r/\bsettings\b/i, query) do
      with {:ok, [statement]} <- Parser.parse_clickhouse_converted(query),
           {:ok, statement, requests} <- extract(statement) do
        enforce_requests(query, statement, inherited, requests, opts)
      else
        {:ok, _} -> {:error, "Expected one ClickHouse SELECT or EXPLAIN SELECT query"}
        error -> error
      end
    else
      enforce_policy(query, inherited, opts)
    end
  end

  @spec enforce_requests(String.t(), map(), map(), map(), keyword()) ::
          {:ok, String.t()} | {:error, String.t()}
  defp enforce_requests(query, _statement, inherited, requests, opts) when requests == %{},
    do: enforce_policy(query, inherited, opts)

  defp enforce_requests(_query, statement, inherited, requests, opts) do
    with :ok <- require_baselines(inherited, requests),
         {:ok, effective} <- ClickHouseSettings.normalize(merge_requests(inherited, requests)),
         {:ok, query} <- Parser.to_string([statement]),
         {:ok, query} <- enforce_policy(query, effective, opts) do
      Logger.debug("Applied lower-only ClickHouse SQL settings",
        clickhouse_requested_settings: inspect(requests),
        clickhouse_effective_settings: inspect(Map.take(effective, Map.keys(requests)))
      )

      {:ok, query}
    end
  end

  @spec enforce_policy(String.t(), map(), keyword()) :: {:ok, String.t()} | {:error, String.t()}
  defp enforce_policy(query, settings, opts) do
    if Keyword.get(opts, :validated_clickhouse_sql, false),
      do: ClickHouseSettings.enforce_validated(query, settings),
      else: ClickHouseSettings.enforce(query, settings)
  end

  @spec require_baselines(map(), map()) :: :ok | {:error, String.t()}
  defp require_baselines(inherited, requests) do
    case Enum.find(Map.keys(requests), &(not Map.has_key?(inherited, &1))) do
      nil -> :ok
      key -> {:error, "ClickHouse setting #{key} requires an explicit inherited policy limit"}
    end
  end

  @spec merge_requests(map(), map()) :: map()
  defp merge_requests(inherited, requests) do
    Map.merge(inherited, requests, fn
      "priority", baseline, requested -> max(baseline, requested)
      _key, baseline, requested -> min(baseline, requested)
    end)
  end

  @spec extract(map()) :: {:ok, map(), map()} | {:error, String.t()}
  defp extract(%{"Explain" => %{"statement" => inner}} = statement) do
    with {:ok, inner, requests} <- extract(inner) do
      {:ok, put_in(statement, ["Explain", "statement"], inner), requests}
    end
  end

  defp extract(%{"Query" => query} = statement) do
    {requests, remaining} =
      Enum.split_with(query["settings"] || [], fn %{"key" => %{"value" => key}} ->
        setting?(String.downcase(key))
      end)

    settings = if remaining == [], do: nil, else: remaining
    statement = put_in(statement, ["Query", "settings"], settings)

    with :ok <- reject_nested(statement),
         {:ok, requests} <- parse_requests(requests) do
      {:ok, statement, requests}
    end
  end

  defp extract(_), do: {:error, "Expected one ClickHouse SELECT or EXPLAIN SELECT query"}

  @spec parse_requests([map()]) :: {:ok, map()} | {:error, String.t()}
  defp parse_requests(nodes) do
    Enum.reduce_while(nodes, {:ok, %{}}, fn %{"key" => %{"value" => key}} = node,
                                            {:ok, requests} ->
      key = String.downcase(key)

      with false <- Map.has_key?(requests, key),
           {:ok, value} <- numeric_literal(key, node["value"]),
           {:ok, _normalized} <- ClickHouseSettings.normalize(%{key => value}) do
        {:cont, {:ok, Map.put(requests, key, value)}}
      else
        true -> {:halt, {:error, "Duplicate ClickHouse setting #{key}"}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  @spec numeric_literal(String.t(), term()) :: {:ok, number()} | {:error, String.t()}
  defp numeric_literal(key, %{"Value" => %{"value" => %{"Number" => [value, false]}}}) do
    parsed =
      case Integer.parse(value) do
        {_integer, ""} = parsed -> parsed
        _ when key == "max_execution_time" -> Float.parse(value)
        _ -> :error
      end

    case parsed do
      {number, ""} -> {:ok, number}
      _ -> {:error, "ClickHouse setting #{key} requires a positive numeric literal"}
    end
  end

  defp numeric_literal(key, _value),
    do: {:error, "ClickHouse setting #{key} requires a positive numeric literal"}

  @spec reject_nested(map()) :: :ok | {:error, String.t()}
  defp reject_nested(statement) do
    case requested_setting(statement) do
      nil -> :ok
      key -> {:error, "ClickHouse setting #{key} is only allowed in the outer SETTINGS clause"}
    end
  end

  @spec requested_setting(term()) :: String.t() | nil
  defp requested_setting(%{"settings" => settings} = node) do
    Enum.find_value(settings || [], fn %{"key" => %{"value" => key}} ->
      normalized = String.downcase(key)
      if setting?(normalized), do: normalized
    end) || requested_setting(Map.delete(node, "settings"))
  end

  defp requested_setting(node) when is_map(node),
    do: Enum.find_value(node, fn {_key, value} -> requested_setting(value) end)

  defp requested_setting(node) when is_list(node), do: Enum.find_value(node, &requested_setting/1)
  defp requested_setting(_node), do: nil
end
