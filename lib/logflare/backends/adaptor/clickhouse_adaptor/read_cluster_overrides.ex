defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.ReadClusterOverrides do
  @moduledoc """
  Ordered, administrator-managed routing rules for ClickHouse endpoint queries.
  """

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryClassSettings
  alias Logflare.Endpoints.EndpointQuery

  @rule_keys ~w(match read_cluster note)

  @spec validate([map()], map(), map()) :: :ok | {:error, String.t()}
  def validate(rules, urls, policies) do
    rules
    |> Enum.with_index(1)
    |> Enum.reduce_while(:ok, fn {rule, index}, :ok ->
      case validate_rule(rule, urls, policies) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, "Rule #{index}: #{reason}"}}
      end
    end)
  end

  @spec validate_rule(map(), map(), map()) :: :ok | {:error, String.t()}
  defp validate_rule(%{"match" => match, "read_cluster" => target} = rule, urls, policies)
       when is_map(match) and map_size(match) > 0 and is_binary(target) and target != "" do
    cond do
      Map.keys(rule) -- @rule_keys != [] ->
        {:error, "unknown rule field"}

      not is_binary(Map.get(rule, "note", "")) ->
        {:error, "note must be a string"}

      not Enum.all?(match, &valid_condition?/1) ->
        {:error, "invalid match condition"}

      target?(target, urls, policies) ->
        :ok

      true ->
        {:error, "target must have a read cluster or query-class policy"}
    end
  end

  defp validate_rule(_rule, _urls, _policies),
    do: {:error, "expected a nonempty match map and a read_cluster"}

  @spec valid_condition?({term(), term()}) :: boolean()
  defp valid_condition?({"endpoint_id", id}), do: is_integer(id) and id > 0

  defp valid_condition?({key, value}),
    do: is_binary(key) and key != "" and is_binary(value) and value != ""

  @spec target?(String.t(), map(), map()) :: boolean()
  defp target?("quarantine", _urls, _policies), do: true

  defp target?(target, urls, policies),
    do:
      Map.has_key?(urls, target) or
        (QueryClassSettings.class_label?(target) and Map.has_key?(policies, target))

  @spec match(map(), EndpointQuery.t() | nil, String.t() | nil) ::
          {String.t(), pos_integer()} | nil
  def match(%{read_cluster_overrides: [_ | _] = rules}, %EndpointQuery{} = endpoint, requested) do
    rules
    |> Enum.with_index(1)
    |> Enum.find_value(fn {%{"match" => conditions, "read_cluster" => target}, index} ->
      if Enum.all?(conditions, &matches?(&1, endpoint, requested)), do: {target, index}
    end)
  end

  def match(_config, _endpoint, _requested), do: nil

  @spec matches?({String.t(), term()}, EndpointQuery.t(), String.t() | nil) :: boolean()
  defp matches?({"endpoint_id", expected}, endpoint, _requested), do: endpoint.id == expected
  defp matches?({"endpoint_name", expected}, endpoint, _requested), do: endpoint.name == expected
  defp matches?({"label", expected}, _endpoint, requested), do: requested == expected

  defp matches?({key, expected}, endpoint, _requested),
    do: Map.get(endpoint.parsed_labels || %{}, key) == expected
end
