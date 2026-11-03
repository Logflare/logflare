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
  @numeric_keys Enum.map(@fields, &elem(&1, 0)) ++
                  ["read_from_filesystem_cache_if_exists_otherwise_bypass_cache"]

  @impl true
  def update(assigns, socket) do
    {:ok,
     socket
     |> assign(assigns)
     |> assign_new(:params, fn -> assigns.backend.config[:query_class_settings] || %{} end)
     |> assign_new(:error, fn -> nil end)
     |> assign_new(:message, fn -> nil end)
     |> assign(:classes, ["default" | QueryClassSettings.classes()])
     |> assign(:fields, @fields)}
  end

  @impl true
  def handle_event("change", %{"query_class_settings" => params}, socket) when is_map(params) do
    case parse_settings(params) do
      {:ok, _settings} -> {:noreply, assign(socket, params: params, error: nil, message: nil)}
      {:error, reason} -> {:noreply, assign(socket, :error, reason)}
    end
  end

  def handle_event("save", %{"query_class_settings" => params}, socket) when is_map(params) do
    case parse_settings(params) do
      {:ok, settings} -> save_settings(settings, assign(socket, :params, params))
      {:error, reason} -> {:noreply, assign(socket, :error, reason)}
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
           settings
         ) do
      {:ok, backend} ->
        message =
          if settings == %{},
            do: "Query-class policy disabled",
            else: "Query-class settings saved"

        {:noreply,
         assign(socket,
           backend: backend,
           params: backend.config.query_class_settings,
           error: nil,
           message: message
         )}

      {:error, %Ecto.Changeset{} = changeset} ->
        {:noreply,
         assign(socket, :error, LogflareWeb.Utils.stringify_changeset_errors(changeset))}

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

  @spec retained_settings(map(), String.t()) :: map()
  defp retained_settings(params, class) do
    Map.take(Map.get(params, class, %{}), [
      "read_from_filesystem_cache_if_exists_otherwise_bypass_cache"
    ])
  end
end
