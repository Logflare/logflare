defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryClassSettings do
  @moduledoc """
  Admin-owned ClickHouse scheduling policy, independent of read-cluster routing.
  An empty map disables class policy. Configured policy requires a default priority.
  """

  alias Logflare.Endpoints.ClickHouseSettings

  @quarantine "quarantine"
  @quarantine_policy %{
    "priority" => 20,
    "max_threads" => 2,
    "max_execution_time" => 10.0,
    "max_bytes_to_read" => 1_073_741_824,
    "timeout_overflow_mode" => "throw",
    "read_overflow_mode" => "throw"
  }
  @classes ~w(dashboard_logs_free dashboard_logs_paid dashboard_reports_free dashboard_reports_paid dashboard_observability mcp api_free api_paid) ++
             [@quarantine]

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
      class_settings = Map.get(classes, requested, %{})

      settings =
        ClickHouseSettings.merge_normalized([
          Map.get(classes, "default", %{}),
          class_settings,
          endpoint_settings
        ])

      {:ok, apply_class_ceiling(requested, settings, class_settings)}
    end
  end

  @spec apply_class_ceiling(term(), map(), map()) :: map()
  defp apply_class_ceiling(@quarantine, settings, class_settings) do
    priority =
      Enum.max([
        Map.get(settings, "priority", 0),
        Map.get(class_settings, "priority", 0),
        @quarantine_policy["priority"]
      ])

    [settings, @quarantine_policy]
    |> ClickHouseSettings.merge_normalized()
    |> Map.put("priority", priority)
  end

  defp apply_class_ceiling(_requested, settings, _class_settings), do: settings

  @spec normalize_classes(map()) :: {:ok, map()} | {:error, String.t()}
  defp normalize_classes(settings) do
    Enum.reduce_while(settings, {:ok, %{}}, fn
      {label, policy}, {:ok, normalized} when label in @classes or label == "default" ->
        case ClickHouseSettings.normalize(policy) do
          {:ok, policy} -> {:cont, {:ok, Map.put(normalized, label, policy)}}
          {:error, reason} -> {:halt, {:error, "Query class #{label}: #{reason}"}}
        end

      {label, _policy}, _acc ->
        {:halt, {:error, "Unknown query class #{inspect(label)}"}}
    end)
  end
end
