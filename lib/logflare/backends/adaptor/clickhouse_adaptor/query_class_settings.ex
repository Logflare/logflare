defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryClassSettings do
  @moduledoc """
  Backend-owned ClickHouse scheduling policy, independent of read-cluster routing.
  An empty map disables class policy. Configured policy requires a default priority.
  """

  import Logflare.Endpoints.ClickHouseSettings, only: [is_valid_timeout: 1]

  alias Logflare.Endpoints.ClickHouseSettings

  @classes ~w(dashboard_logs_free dashboard_logs_paid dashboard_reports_free dashboard_reports_paid dashboard_observability mcp api_free api_paid)

  @spec classes() :: [String.t()]
  def classes, do: @classes

  @spec class_label?(term()) :: boolean()
  def class_label?(label), do: label in @classes

  @spec normalize(term()) :: {:ok, map()} | {:error, String.t()}
  def normalize(settings) when settings == %{}, do: {:ok, settings}

  def normalize(settings) when is_map(settings) do
    with {:ok, normalized} <- normalize_classes(settings),
         %{"priority" => priority} when is_integer(priority) <- normalized["default"] do
      {:ok, normalized}
    else
      {:error, _} = error -> error
      _ -> {:error, "Query class settings require a default with a positive priority"}
    end
  end

  def normalize(_), do: {:error, "Query class settings must be a map"}

  @spec resolve(map(), term(), map()) :: {:ok, map()} | {:error, String.t()}
  def resolve(classes, requested, endpoint_settings) do
    with {:ok, endpoint_settings} <- ClickHouseSettings.normalize(endpoint_settings) do
      {:ok,
       ClickHouseSettings.merge_normalized([
         classes |> Map.get("default", %{}) |> discard_invalid_timeout(),
         classes |> Map.get(requested, %{}) |> discard_invalid_timeout(),
         endpoint_settings
       ])}
    end
  end

  @spec discard_invalid_timeout(map()) :: map()
  defp discard_invalid_timeout(%{"max_execution_time" => seconds} = policy)
       when not is_valid_timeout(seconds),
       do: Map.delete(policy, "max_execution_time")

  defp discard_invalid_timeout(policy), do: policy

  @spec normalize_classes(map()) :: {:ok, map()} | {:error, String.t()}
  defp normalize_classes(settings) do
    Enum.reduce_while(settings, {:ok, %{}}, fn
      {label, policy}, {:ok, normalized} when label in @classes or label == "default" ->
        case normalize_class(policy) do
          {:ok, policy} -> {:cont, {:ok, Map.put(normalized, label, policy)}}
          {:error, reason} -> {:halt, {:error, "Query class #{label}: #{reason}"}}
        end

      {label, _policy}, _acc ->
        {:halt, {:error, "Unknown query class #{inspect(label)}"}}
    end)
  end

  @spec normalize_class(term()) :: {:ok, map()} | {:error, String.t()}
  defp normalize_class(%{"max_execution_time" => seconds}) when not is_valid_timeout(seconds),
    do: {:error, "max_execution_time must fit in a positive signed 64-bit microsecond duration"}

  defp normalize_class(policy), do: ClickHouseSettings.normalize(policy)
end
