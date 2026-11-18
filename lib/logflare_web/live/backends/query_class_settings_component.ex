defmodule LogflareWeb.Backends.QueryClassSettingsComponent do
  use LogflareWeb, :live_component

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryClassSettings

  @fields [
    {"priority", "Priority"},
    {"max_threads", "Max threads"},
    {"max_memory_usage", "Max memory usage (bytes)"},
    {"max_execution_time", "Max execution time (seconds)"},
    {"max_rows_to_read", "Max rows to read"},
    {"max_bytes_to_read", "Max bytes to read"}
  ]
  @numeric_keys Enum.map(@fields, &elem(&1, 0))
  @override_classes QueryClassSettings.classes()
  @cache_key "read_from_filesystem_cache_if_exists_otherwise_bypass_cache"
  @overflow_mode_limits [
    {"read_overflow_mode", ~w(max_rows_to_read max_bytes_to_read)},
    {"timeout_overflow_mode", ~w(max_execution_time)}
  ]

  @impl true
  def update(assigns, socket) do
    {:ok,
     socket
     |> assign(assigns)
     |> assign_new(:original_policy, fn ->
       assigns.backend.config[:query_class_settings] || %{}
     end)
     |> assign_new(:params, fn -> assigns.backend.config[:query_class_settings] || %{} end)
     |> assign_new(:error, fn -> nil end)
     |> assign_new(:message, fn -> nil end)
     |> assign_new(:reset_classes, &MapSet.new/0)
     |> assign(:classes, ["default" | @override_classes])
     |> assign(:fields, @fields)}
  end

  @impl true
  def handle_event("change", %{"query_class_settings" => params}, socket) when is_map(params) do
    case parse_settings(params) do
      {:ok, _settings} -> {:noreply, assign(socket, params: params, error: nil, message: nil)}
      {:error, reason} -> {:noreply, assign(socket, :error, reason)}
    end
  end

  def handle_event("save", %{"reset_class" => class, "query_class_settings" => params}, socket)
      when class in @override_classes and is_map(params) do
    case parse_settings(params) do
      {:ok, _settings} ->
        {:noreply,
         assign(socket,
           params: Map.delete(params, class),
           reset_classes: MapSet.put(socket.assigns.reset_classes, class),
           error: nil,
           message: "Class reset in this draft. Save query-class settings to apply it."
         )}

      {:error, reason} ->
        {:noreply, assign(socket, :error, reason)}
    end
  end

  def handle_event("save", %{"query_class_settings" => params} = payload, socket)
      when is_map(params) and not is_map_key(payload, "reset_class") do
    case parse_settings(params) do
      {:ok, settings} ->
        settings = restore_hidden_overrides(settings, socket.assigns)
        save_settings(settings, assign(socket, :params, params))

      {:error, reason} ->
        {:noreply, assign(socket, :error, reason)}
    end
  end

  def handle_event("clear", _params, socket), do: save_settings(%{}, socket)

  def handle_event(event, _params, socket) when event in ["change", "save"] do
    {:noreply, assign(socket, :error, "Invalid query-class form. Reload and try again.")}
  end

  @spec save_settings(map(), Phoenix.LiveView.Socket.t()) ::
          {:noreply, Phoenix.LiveView.Socket.t()}
  defp save_settings(settings, socket) do
    socket = assign(socket, :message, nil)

    case Backends.configure_query_class_settings(
           socket.assigns.actor,
           socket.assigns.backend,
           settings,
           expected_policy: socket.assigns.original_policy
         ) do
      {:ok, backend} ->
        policy = backend.config.query_class_settings

        message =
          if settings == %{},
            do: "Query-class policy disabled",
            else: "Query-class settings saved"

        {:noreply,
         assign(socket,
           backend: backend,
           original_policy: policy,
           params: policy,
           reset_classes: MapSet.new(),
           error: nil,
           message: message
         )}

      {:error, %Ecto.Changeset{} = changeset} ->
        {:noreply,
         assign(socket, :error, LogflareWeb.Utils.stringify_changeset_errors(changeset))}

      {:error, :stale_policy} ->
        {:noreply,
         assign(
           socket,
           :error,
           "The query-class policy changed. Reload and reapply your changes."
         )}

      {:error, :forbidden} ->
        {:noreply, assign(socket, :error, "You no longer have permission to edit this backend.")}

      {:error, _reason} ->
        {:noreply,
         assign(socket, :error, "Unable to save query-class settings. Reload and try again.")}
    end
  end

  @spec parse_settings(map()) :: {:ok, map()} | {:error, String.t()}
  defp parse_settings(params) do
    Enum.reduce_while(params, {:ok, %{}}, fn
      {class, values}, {:ok, settings} when is_map(values) ->
        policy =
          values
          |> Map.take(@numeric_keys)
          |> Enum.reject(fn {_key, value} -> value in ["", nil] end)
          |> Map.new(fn {key, value} -> {key, parse_value(key, value)} end)

        settings = if policy == %{}, do: settings, else: Map.put(settings, class, policy)
        {:cont, {:ok, settings}}

      _entry, _acc ->
        {:halt, {:error, "Query-class settings must contain named classes and numeric fields."}}
    end)
  end

  @spec parse_value(String.t(), term()) :: term()
  defp parse_value("max_execution_time", value) when is_binary(value) do
    case Float.parse(value) do
      {number, ""} -> number
      _other -> value
    end
  end

  defp parse_value(key, value) when key in @numeric_keys and is_binary(value) do
    case Integer.parse(value) do
      {number, ""} -> number
      _other -> value
    end
  end

  defp parse_value(_key, value), do: value

  @spec class_form(map(), String.t()) :: Phoenix.HTML.Form.t()
  defp class_form(params, class) do
    to_form(Map.get(params, class, %{}),
      as: "query_class_settings[#{class}]",
      id: "query-class-#{class}"
    )
  end

  @spec restore_hidden_overrides(map(), map()) :: map()
  defp restore_hidden_overrides(settings, %{
         original_policy: original,
         reset_classes: reset_classes
       }) do
    Enum.reduce(Map.drop(original, MapSet.to_list(reset_classes)), settings, fn {class, policy},
                                                                                acc ->
      case hidden_overrides(policy) do
        overrides when overrides == %{} -> acc
        overrides -> Map.update(acc, class, overrides, &Map.merge(&1, overrides))
      end
    end)
  end

  @spec hidden_overrides(map()) :: map()
  defp hidden_overrides(policy) do
    standalone_modes =
      for {mode, limits} <- @overflow_mode_limits,
          not Enum.any?(limits, &Map.has_key?(policy, &1)),
          do: mode

    Map.take(policy, [@cache_key | standalone_modes])
  end

  @spec cache_override(map(), MapSet.t(String.t()), String.t()) :: 0 | 1 | nil
  defp cache_override(original, reset_classes, class) do
    unless MapSet.member?(reset_classes, class), do: get_in(original, [class, @cache_key])
  end
end
