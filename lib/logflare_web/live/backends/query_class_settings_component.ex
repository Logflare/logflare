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
  @invalid_params_shape "Query-class settings must contain named classes and numeric fields."
  @cache_key "read_from_filesystem_cache_if_exists_otherwise_bypass_cache"
  @hidden_keys [@cache_key, "read_overflow_mode", "timeout_overflow_mode"]

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
     |> assign_new(:input_revision, &Ecto.UUID.generate/0)
     |> assign_new(:class_resets, fn -> %{} end)
     |> assign(:classes, ["default" | @override_classes])
     |> assign(:fields, @fields)}
  end

  @impl true
  def handle_event("save", %{"reset_class" => class, "query_class_settings" => params}, socket)
      when class in @override_classes and is_map(params) do
    if Enum.all?(params, fn {_class, fields} -> is_map(fields) end) do
      {:noreply,
       assign(socket,
         params: Map.delete(params, class),
         class_resets: Map.put(socket.assigns.class_resets, class, Ecto.UUID.generate()),
         error: nil,
         message: "Class reset in this draft. Save query-class settings to apply it."
       )}
    else
      {:noreply, assign(socket, :error, @invalid_params_shape)}
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

  def handle_event("save", _params, socket) do
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
           input_revision: input_revision_after_save(policy, socket.assigns.input_revision),
           class_resets: %{},
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

  @spec input_revision_after_save(map(), String.t()) :: String.t()
  defp input_revision_after_save(policy, _revision) when policy == %{}, do: Ecto.UUID.generate()
  defp input_revision_after_save(_policy, revision), do: revision

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
        {:halt, {:error, @invalid_params_shape}}
    end)
  end

  @spec parse_value(String.t(), term()) :: term()
  defp parse_value("max_execution_time", "." <> value),
    do: parse_value("max_execution_time", "0." <> value)

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

  @spec class_form(map(), String.t(), String.t()) :: Phoenix.HTML.Form.t()
  defp class_form(params, class, revision) do
    to_form(Map.get(params, class, %{}),
      as: "query_class_settings[#{class}]",
      id: "query-class-#{class}-#{revision}"
    )
  end

  @spec restore_hidden_overrides(map(), map()) :: map()
  defp restore_hidden_overrides(settings, %{
         original_policy: original,
         class_resets: class_resets
       }) do
    Enum.reduce(Map.drop(original, Map.keys(class_resets)), settings, fn {class, policy}, acc ->
      case Map.take(policy, @hidden_keys) do
        overrides when overrides == %{} -> acc
        overrides -> Map.update(acc, class, overrides, &Map.merge(&1, overrides))
      end
    end)
  end

  @spec cache_override(map(), map(), String.t()) :: 0 | 1 | nil
  defp cache_override(original, class_resets, class) do
    unless Map.has_key?(class_resets, class), do: get_in(original, [class, @cache_key])
  end
end
